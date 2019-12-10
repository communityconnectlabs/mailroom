package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeSessionTriggered, handleSessionTriggered)
}

// StartStartHook is our hook to fire our session starts
type StartStartHook struct{}

var startStartHook = &StartStartHook{}

// InsertStartHook is our hook to fire insert our starts
type InsertStartHook struct{}

var insertStartHook = &InsertStartHook{}

// Apply queues up our flow starts
func (h *StartStartHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// for each of our sessions
	for _, es := range sessions {
		for _, e := range es {
			start := e.(*models.FlowStart)

			taskQ := queue.HandlerQueue
			priority := queue.DefaultPriority

			// if we are starting groups, queue to our batch queue instead, but with high priority
			if len(start.GroupIDs()) > 0 {
				taskQ = queue.BatchQueue
				priority = queue.HighPriority
			}

			err := queue.AddTask(rc, taskQ, queue.StartFlow, int(org.OrgID()), start, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing flow start")
			}
		}
	}

	return nil
}

// Apply inserts our starts
func (h *InsertStartHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	starts := make([]*models.FlowStart, 0, len(sessions))

	// for each of our sessions
	for s, es := range sessions {
		for _, e := range es {
			event := e.(*events.SessionTriggeredEvent)

			// we skip over any session starts that involve groups if we are in a batch start
			if len(sessions) > 1 && len(event.Groups) > 0 {
				logrus.WithField("session_id", s.ID).Error("ignoring session trigger on group in batch")
				continue
			}

			// look up our flow
			f, err := org.Flow(event.Flow.UUID)
			if err != nil {
				return errors.Wrapf(err, "unable to load flow with UUID: %s", event.Flow.UUID)
			}
			flow := f.(*models.Flow)

			// load our groups by uuid
			groupIDs := make([]models.GroupID, 0, len(event.Groups))
			for i := range event.Groups {
				group := org.GroupByUUID(event.Groups[i].UUID)
				if group != nil {
					groupIDs = append(groupIDs, group.ID())
				}
			}

			// load our contacts by uuid
			contactIDs, err := models.ContactIDsFromReferences(ctx, tx, org, event.Contacts)
			if err != nil {
				return errors.Wrapf(err, "error loading contacts by reference")
			}

			// create our start
			start := models.NewFlowStart(
				org.OrgID(), flow.FlowType(), flow.ID(),
				groupIDs, contactIDs, event.URNs, event.CreateContact,
				true, true,
				event.RunSummary, nil,
			)

			starts = append(starts, start)

			// this will add our task for our start after we commit
			s.AddPostCommitEvent(startStartHook, start)
		}
	}

	// insert all our starts
	err := models.InsertFlowStarts(ctx, tx, starts)
	if err != nil {
		return errors.Wrapf(err, "error inserting flow starts for session triggers")
	}

	return nil
}

// handleSessionTriggered queues this event for being started after our sessions are committed
func handleSessionTriggered(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.SessionTriggeredEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"flow":         event.Flow.Name,
		"flow_uuid":    event.Flow.UUID,
	}).Debug("session triggered")

	session.AddPreCommitEvent(insertStartHook, event)

	return nil
}
