package starts

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/greatnonprofits-nfp/goflow/contactql"
	"github.com/greatnonprofits-nfp/goflow/envs"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/runner"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	startBatchSize = 100
)

func init() {
	mailroom.AddTaskFunction(queue.StartFlow, handleFlowStart)
	mailroom.AddTaskFunction(queue.StartFlowBatch, handleFlowStartBatch)
	mailroom.AddTaskFunction(queue.StartStudioFlow, handleStudioFlowStart)
}

// handleFlowStart creates all the batches of contacts to start in a flow
func handleFlowStart(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*60)
	defer cancel()

	// decode our task body
	if task.Type != queue.StartFlow {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startTask := &models.FlowStart{}
	err := json.Unmarshal(task.Task, startTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start task: %s", string(task.Task))
	}

	err = CreateFlowBatches(ctx, mr.DB, mr.RP, mr.ElasticClient, startTask)
	if err != nil {
		models.MarkStartFailed(ctx, mr.DB, startTask.ID())

		// if error is user created query error.. don't escalate error to sentry
		isQueryError, _ := contactql.IsQueryError(err)
		if !isQueryError {
			return err
		}
	}

	return nil
}

// CreateFlowBatches takes our master flow start and creates batches of flow starts for all the unique contacts
func CreateFlowBatches(ctx context.Context, db *sqlx.DB, rp *redis.Pool, ec *elastic.Client, start *models.FlowStart) error {
	contactIDs := make(map[models.ContactID]bool)
	createdContactIDs := make([]models.ContactID, 0)

	// we are building a set of contact ids, start with the explicit ones
	for _, id := range start.ContactIDs() {
		contactIDs[id] = true
	}

	oa, err := models.GetOrgAssets(ctx, db, start.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org assets")
	}

	// look up any contacts by URN
	if len(start.URNs()) > 0 {
		urnContactIDs, err := models.GetOrCreateContactIDsFromURNs(ctx, db, oa, start.URNs())
		if err != nil {
			return errors.Wrapf(err, "error getting contact ids from urns")
		}
		for _, id := range urnContactIDs {
			if !contactIDs[id] {
				createdContactIDs = append(createdContactIDs, id)
			}
			contactIDs[id] = true
		}
	}

	// if we are meant to create a new contact, do so
	if start.CreateContact() {
		contact, _, err := models.CreateContact(ctx, db, oa, models.NilUserID, "", envs.NilLanguage, nil)
		if err != nil {
			return errors.Wrapf(err, "error creating new contact")
		}
		contactIDs[contact.ID()] = true
		createdContactIDs = append(createdContactIDs, contact.ID())
	}

	// now add all the ids for our groups
	if len(start.GroupIDs()) > 0 {
		rows, err := db.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(start.GroupIDs()))
		if err != nil {
			return errors.Wrapf(err, "error selecting contacts for groups")
		}
		defer rows.Close()

		var contactID models.ContactID
		for rows.Next() {
			err := rows.Scan(&contactID)
			if err != nil {
				return errors.Wrapf(err, "error scanning contact id")
			}
			contactIDs[contactID] = true
		}
	}

	// finally, if we have a query, add the contacts that match that as well
	if start.Query() != "" {
		matches, err := models.ContactIDsForQuery(ctx, ec, oa, start.Query())
		if err != nil {
			return errors.Wrapf(err, "error performing search for start: %d", start.ID())
		}

		for _, contactID := range matches {
			contactIDs[contactID] = true
		}
	}

	rc := rp.Get()
	defer rc.Close()

	// mark our start as starting, last task will mark as complete
	err = models.MarkStartStarted(ctx, db, start.ID(), len(contactIDs), createdContactIDs)
	if err != nil {
		return errors.Wrapf(err, "error marking start as started")
	}

	// if there are no contacts to start, mark our start as complete, we are done
	if len(contactIDs) == 0 {
		err = models.MarkStartComplete(ctx, db, start.ID())
		if err != nil {
			return errors.Wrapf(err, "error marking start as complete")
		}
		return nil
	}

	// by default we start in the batch queue unless we have two or fewer contacts
	q := queue.BatchQueue
	if len(contactIDs) <= 2 {
		q = queue.HandlerQueue
	}

	// task is different if we are an IVR flow
	taskType := queue.StartFlowBatch
	if start.FlowType() == models.FlowTypeVoice {
		taskType = queue.StartIVRFlowBatch
	}

	contacts := make([]models.ContactID, 0, 100)
	queueBatch := func(last bool) {
		batch := start.CreateBatch(contacts, last, len(contactIDs))
		err = queue.AddTask(rc, q, taskType, int(start.OrgID()), batch, queue.DefaultPriority)
		if err != nil {
			// TODO: is continuing the right thing here? what do we do if redis is down? (panic!)
			logrus.WithError(err).WithField("start_id", start.ID()).Error("error while queuing start")
		}
		contacts = make([]models.ContactID, 0, 100)
	}

	// build up batches of contacts to start
	for c := range contactIDs {
		if len(contacts) == startBatchSize {
			queueBatch(false)
		}
		contacts = append(contacts, c)
	}

	// queue our last batch
	if len(contacts) > 0 {
		queueBatch(true)
	}

	return nil
}

// HandleFlowStartBatch starts a batch of contacts in a flow
func handleFlowStartBatch(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	// decode our task body
	if task.Type != queue.StartFlowBatch {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startBatch := &models.FlowStartBatch{}
	err := json.Unmarshal(task.Task, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start batch: %s", string(task.Task))
	}

	// start these contacts in our flow
	_, err = runner.StartFlowBatch(ctx, mr.DB, mr.RP, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error starting flow batch: %s", string(task.Task))
	}

	return err
}

type RequestSender interface {
	Do(*http.Request) (*http.Response, error)
}

var requestSender RequestSender = http.DefaultClient

func handleStudioFlowStart(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	db := mr.DB
	ctx, cancel := context.WithTimeout(ctx, time.Minute*60)
	defer cancel()

	startTask := &models.StudioFlowStart{}
	err := json.Unmarshal(task.Task, startTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling studio flow start task: %s", string(task.Task))
	}

	accountSID, accountToken, err := startTask.LoadTwilioConfig(ctx, db)
	if err != nil {
		return errors.Wrapf(err, "error loading studio flow start channel")
	}

	if accountSID == "" {
		return errors.Wrapf(err, "missing account sid for %d org", task.OrgID)
	}

	if accountToken == "" {
		return errors.Wrapf(err, "missing account auth token for %d org", task.OrgID)
	}

	contactIDsSet := make(map[models.ContactID]bool)
	// we are building a set of contact ids, start with the explicit ones
	for _, id := range startTask.ContactIDs() {
		contactIDsSet[id] = true
	}

	// now add all the ids for our groups
	if len(startTask.GroupIDs()) > 0 {
		rows, err := db.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(startTask.GroupIDs()))
		if err != nil {
			return errors.Wrapf(err, "error selecting contacts for groups")
		}
		defer rows.Close()

		var contactID models.ContactID
		for rows.Next() {
			err := rows.Scan(&contactID)
			if err != nil {
				return errors.Wrapf(err, "error scanning contact id")
			}
			contactIDsSet[contactID] = true
		}
	}

	// skip if there is no contacts selected
	if len(contactIDsSet) == 0 {
		return nil
	}

	contactIDs := make([]int64, 0, len(contactIDsSet))
	for contactID := range contactIDsSet {
		contactIDs = append(contactIDs, int64(contactID))
	}

	// 80 mps limiting for the twilio
	chunkSize := 80
	chunkNumber := 0
	successCount := 0
	failureCount := 0
	totalContactIDs := len(contactIDs)
	contactIDChunkSelector := func(chunkIndex int) []int64 {
		start := chunkIndex * chunkSize
		end := start + chunkSize
		if start > totalContactIDs {
			return []int64{}
		}
		if end > totalContactIDs {
			end = totalContactIDs
		}
		return contactIDs[start:end]
	}
	sendURL := fmt.Sprintf("https://studio.twilio.com/v2/Flows/%s/Executions", startTask.FlowSID())
	for range time.Tick(1 * time.Second) {
		contactIDsChunk := contactIDChunkSelector(chunkNumber)
		if len(contactIDsChunk) == 0 {
			break
		}

		contactPhones, err := startTask.LoadContactPhones(ctx, db, contactIDsChunk)
		if err != nil {
			startTask.MarkStartFailed(ctx, db)
			return errors.Wrapf(err, "error getting contact urns")
		}

		// send requests to twilio
		for _, phone := range contactPhones {
			form := url.Values{
				"To":   []string{phone},
				"From": []string{startTask.Channel()},
			}

			req, err := http.NewRequest(http.MethodPost, sendURL, strings.NewReader(form.Encode()))
			if err != nil {
				startTask.MarkStartFailed(ctx, db)
				return err
			}
			req.SetBasicAuth(accountSID, accountToken)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Accept", "application/json")

			resp, err := requestSender.Do(req)
			if err != nil || resp.StatusCode != 201 {
				failureCount++
			} else {
				successCount++
			}
		}
		chunkNumber++

		startTask.WithMetadata(map[string]interface{}{
			"total_contacts":    totalContactIDs,
			"success_count":     successCount,
			"failure_count":     failureCount,
			"processed_batches": chunkNumber,
			"batch_size":        chunkSize,
		}).UpdateMetadata(ctx, db)
	}
	return startTask.MarkStartComplete(ctx, db)
}
