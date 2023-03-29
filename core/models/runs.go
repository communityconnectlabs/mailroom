package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
)

type FlowRunID int64

const NilFlowRunID = FlowRunID(0)

type RunStatus string

const (
	RunStatusActive      RunStatus = "A"
	RunStatusWaiting     RunStatus = "W"
	RunStatusCompleted   RunStatus = "C"
	RunStatusExpired     RunStatus = "X"
	RunStatusInterrupted RunStatus = "I"
	RunStatusFailed      RunStatus = "F"
)

var runStatusMap = map[flows.RunStatus]RunStatus{
	flows.RunStatusActive:    RunStatusActive,
	flows.RunStatusWaiting:   RunStatusWaiting,
	flows.RunStatusCompleted: RunStatusCompleted,
	flows.RunStatusExpired:   RunStatusExpired,
	flows.RunStatusFailed:    RunStatusFailed,
}

// ExitType still needs to be set on runs until database triggers are updated to only look at status
type ExitType = null.String

const (
	ExitInterrupted = ExitType("I")
	ExitCompleted   = ExitType("C")
	ExitExpired     = ExitType("E")
	ExitFailed      = ExitType("F")
)

var runStatusToExitType = map[RunStatus]ExitType{
	RunStatusInterrupted: ExitInterrupted,
	RunStatusCompleted:   ExitCompleted,
	RunStatusExpired:     ExitExpired,
	RunStatusFailed:      ExitFailed,
}

// FlowRun is the mailroom type for a FlowRun
type FlowRun struct {
	r struct {
		ID              FlowRunID       `db:"id"`
		UUID            flows.RunUUID   `db:"uuid"`
		Status          RunStatus       `db:"status"`
		CreatedOn       time.Time       `db:"created_on"`
		ModifiedOn      time.Time       `db:"modified_on"`
		ExitedOn        *time.Time      `db:"exited_on"`
		Responded       bool            `db:"responded"`
		Results         string          `db:"results"`
		Path            string          `db:"path"`
		CurrentNodeUUID null.String     `db:"current_node_uuid"`
		ContactID       flows.ContactID `db:"contact_id"`
		FlowID          FlowID          `db:"flow_id"`
		OrgID           OrgID           `db:"org_id"`
		SessionID       SessionID       `db:"session_id"`
		StartID         StartID         `db:"start_id"`
	}

	// we keep a reference to the engine's run
	run flows.Run
}

func (r *FlowRun) SetSessionID(sessionID SessionID) { r.r.SessionID = sessionID }
func (r *FlowRun) SetStartID(startID StartID)       { r.r.StartID = startID }
func (r *FlowRun) UUID() flows.RunUUID              { return r.r.UUID }
func (r *FlowRun) ModifiedOn() time.Time            { return r.r.ModifiedOn }

// MarshalJSON is our custom marshaller so that our inner struct get output
func (r *FlowRun) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.r)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (r *FlowRun) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &r.r)
}

// Step represents a single step in a run, this struct is used for serialization to the steps
type Step struct {
	UUID      flows.StepUUID `json:"uuid"`
	NodeUUID  flows.NodeUUID `json:"node_uuid"`
	ArrivedOn time.Time      `json:"arrived_on"`
	ExitUUID  flows.ExitUUID `json:"exit_uuid,omitempty"`
}

const sqlInsertRun = `
INSERT INTO
flows_flowrun(uuid, created_on, modified_on, exited_on, status, responded, results, path, 
	          current_node_uuid, contact_id, flow_id, org_id, session_id, start_id)
	   VALUES(:uuid, :created_on, NOW(), :exited_on, :status, :responded, :results, :path,
	          :current_node_uuid, :contact_id, :flow_id, :org_id, :session_id, :start_id)
RETURNING id
`

// newRun writes the passed in flow run to our database, also applying any events in those runs as
// appropriate. (IE, writing db messages etc..)
func newRun(ctx context.Context, tx *sqlx.Tx, oa *OrgAssets, session *Session, fr flows.Run) (*FlowRun, error) {
	// build our path elements
	path := make([]Step, len(fr.Path()))
	for i, p := range fr.Path() {
		path[i].UUID = p.UUID()
		path[i].NodeUUID = p.NodeUUID()
		path[i].ArrivedOn = p.ArrivedOn()
		path[i].ExitUUID = p.ExitUUID()
	}

	flowID, err := FlowIDForUUID(ctx, tx, oa, fr.FlowReference().UUID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load flow with uuid: %s", fr.FlowReference().UUID)
	}

	// create our run
	run := &FlowRun{}
	r := &run.r
	r.UUID = fr.UUID()
	r.Status = runStatusMap[fr.Status()]
	r.CreatedOn = fr.CreatedOn()
	r.ExitedOn = fr.ExitedOn()
	r.ModifiedOn = fr.ModifiedOn()
	r.ContactID = fr.Contact().ID()
	r.FlowID = flowID
	r.SessionID = session.ID()
	r.StartID = NilStartID
	r.OrgID = oa.OrgID()
	r.Path = string(jsonx.MustMarshal(path))
	r.Results = string(jsonx.MustMarshal(fr.Results()))

	if len(path) > 0 {
		r.CurrentNodeUUID = null.String(path[len(path)-1].NodeUUID)
	}
	run.run = fr

	// mark ourselves as responded if we received a message
	for _, e := range fr.Events() {
		if e.Type() == events.TypeMsgReceived {
			r.Responded = true
			break
		}
	}

	return run, nil
}

// FindFlowStartedOverlap returns the list of contact ids which overlap with those passed in and which
// have been in the flow passed in.
func FindFlowStartedOverlap(ctx context.Context, db *sqlx.DB, flowID FlowID, contacts []ContactID) ([]ContactID, error) {
	var overlap []ContactID
	err := db.SelectContext(ctx, &overlap, flowStartedOverlapSQL, pq.Array(contacts), flowID)
	return overlap, err
}

// TODO: no perfect index, will probably use contact index flows_flowrun_contact_id_985792a9
// could be slow in the cases of contacts having many distinct runs
const flowStartedOverlapSQL = `
SELECT
	DISTINCT(contact_id)
FROM
	flows_flowrun
WHERE
	contact_id = ANY($1) AND
	flow_id = $2
`

// FindActiveSessionOverlap returns the list of contact ids which overlap with those passed in which are active in any other flows
func FindActiveSessionOverlap(ctx context.Context, db *sqlx.DB, flowType FlowType, contacts []ContactID) ([]ContactID, error) {
	// background flows should look at messaging flows when determing overlap (background flows can't be active by definition)
	if flowType == FlowTypeBackground {
		flowType = FlowTypeMessaging
	}

	var overlap []ContactID
	err := db.SelectContext(ctx, &overlap, activeSessionOverlapSQL, flowType, pq.Array(contacts))
	return overlap, err
}

const activeSessionOverlapSQL = `
SELECT
	DISTINCT(contact_id)
FROM
	flows_flowsession fs JOIN
	flows_flow ff ON fs.current_flow_id = ff.id
WHERE
	fs.status = 'W' AND
	ff.is_active = TRUE AND
	ff.is_archived = FALSE AND
	ff.flow_type = $1 AND
	fs.contact_id = ANY($2)
`

// RunExpiration looks up the run expiration for the passed in run, can return nil if the run is no longer active
func RunExpiration(ctx context.Context, db *sqlx.DB, runID FlowRunID) (*time.Time, error) {
	var expiration time.Time
	err := db.Get(&expiration, `SELECT expires_on FROM flows_flowrun WHERE id = $1 AND is_active = TRUE`, runID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "unable to select expiration for run: %d", runID)
	}
	return &expiration, nil
}

// ExitSessions marks the passed in sessions as completed, also doing so for all associated runs
func ExitSessions(ctx context.Context, tx Queryer, sessionIDs []SessionID, exitType ExitType, now time.Time) error {
	if len(sessionIDs) == 0 {
		return nil
	}

	// map exit type to statuses for sessions and runs
	sessionStatus := exitToSessionStatusMap[exitType]
	runStatus, found := exitToRunStatusMap[exitType]
	if !found {
		return errors.Errorf("unknown exit type: %s", exitType)
	}

	// first interrupt our runs
	start := time.Now()
	res, err := tx.ExecContext(ctx, exitSessionRunsSQL, pq.Array(sessionIDs), exitType, now, runStatus)
	if err != nil {
		return errors.Wrapf(err, "error exiting session runs")
	}
	rows, _ := res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("exited session runs")

	// then our sessions
	start = time.Now()

	res, err = tx.ExecContext(ctx, exitSessionsSQL, pq.Array(sessionIDs), now, sessionStatus)
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}
	rows, _ = res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("exited sessions")

	return nil
}

const exitSessionRunsSQL = `
UPDATE
	flows_flowrun
SET
	is_active = FALSE,
	exit_type = $2,
	exited_on = $3,
	status = $4,
	modified_on = NOW()
WHERE
	id = ANY (SELECT id FROM flows_flowrun WHERE session_id = ANY($1) AND is_active = TRUE)
`

const exitSessionsSQL = `
UPDATE
	flows_flowsession
SET
	ended_on = $2,
	status = $3
WHERE
	id = ANY ($1) AND
	status = 'W'
`

// InterruptContactRuns interrupts all runs and sesions that exist for the passed in list of contacts
func InterruptContactRuns(ctx context.Context, tx Queryer, sessionType FlowType, contactIDs []flows.ContactID, now time.Time) error {
	if len(contactIDs) == 0 {
		return nil
	}

	// first interrupt our runs
	err := Exec(ctx, "interrupting contact runs", tx, interruptContactRunsSQL, sessionType, pq.Array(contactIDs), now)
	if err != nil {
		return err
	}

	err = Exec(ctx, "interrupting contact sessions", tx, interruptContactSessionsSQL, sessionType, pq.Array(contactIDs), now)
	if err != nil {
		return err
	}

	return nil
}

const interruptContactRunsSQL = `
UPDATE
	flows_flowrun
SET
	is_active = FALSE,
	exited_on = $3,
	exit_type = 'I',
	status = 'I',
	modified_on = NOW()
WHERE
	id = ANY (
		SELECT 
		  fr.id 
		FROM 
		  flows_flowrun fr
		  JOIN flows_flow ff ON fr.flow_id = ff.id
		WHERE 
		  fr.contact_id = ANY($2) AND 
		  fr.is_active = TRUE AND
		  ff.flow_type = $1
		)
`

const interruptContactSessionsSQL = `
UPDATE
	flows_flowsession
SET
	status = 'I',
	ended_on = $3
WHERE
	id = ANY (SELECT id FROM flows_flowsession WHERE session_type = $1 AND contact_id = ANY($2) AND status = 'W')
`

// ExpireRunsAndSessions expires all the passed in runs and sessions. Note this should only be called
// for runs that have no parents or no way of continuing
func ExpireRunsAndSessions(ctx context.Context, db *sqlx.DB, runIDs []FlowRunID, sessionIDs []SessionID) error {
	if len(runIDs) == 0 {
		return nil
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "error starting transaction to expire sessions")
	}

	err = Exec(ctx, "expiring runs", tx, expireRunsSQL, pq.Array(runIDs))
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error expiring runs")
	}

	if len(sessionIDs) > 0 {
		err = Exec(ctx, "expiring sessions", tx, expireSessionsSQL, pq.Array(sessionIDs))
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error expiring sessions")
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "error committing expiration of runs and sessions")
	}
	return nil
}

const expireSessionsSQL = `
	UPDATE
		flows_flowsession s
	SET
		timeout_on = NULL,
		ended_on = NOW(),
		status = 'X'
	WHERE
		id = ANY($1)
`

const expireRunsSQL = `
	UPDATE
		flows_flowrun fr
	SET
		is_active = FALSE,
		exited_on = NOW(),
		exit_type = 'E',
		status = 'E',
		modified_on = NOW()
	WHERE
		id = ANY($1)
`

// NewEmptyRun enables to create an empty run, without results, only to log the contact interaction
func NewEmptyRun(ctx context.Context, db Queryer, contactID flows.ContactID, flowID FlowID, orgID OrgID) error {
	run := &FlowRun{}
	r := &run.r
	r.UUID = flows.RunUUID(uuids.New())
	r.Status = RunStatusCompleted
	r.CreatedOn = time.Now()
	r.ExpiresOn = nil
	r.ModifiedOn = time.Now()
	r.ContactID = contactID
	r.FlowID = flowID
	r.StartID = NilStartID
	r.OrgID = orgID
	r.IsActive = false
	r.ExitType = ExitCompleted
	r.Responded = false

	filteredEvents := make([]flows.Event, 0)
	eventJSON, err := json.Marshal(filteredEvents)
	if err != nil {
		return errors.Wrapf(err, "error marshalling events for run on creating empty run")
	}
	r.Events = string(eventJSON)

	path := make([]Step, 0)
	pathJSON, err := json.Marshal(path)
	if err != nil {
		return err
	}
	r.Path = string(pathJSON)

	results := make(flows.Results)
	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return errors.Wrapf(err, "error marshalling results for run on creating empty run")
	}
	r.Results = string(resultsJSON)

	exitedOn := time.Now().Add(time.Second * 3)

	_, err = db.ExecContext(ctx,
		`
			INSERT INTO
				flows_flowrun(uuid, is_active, created_on, modified_on, exited_on, exit_type, status, expires_on, responded, results, path, events, current_node_uuid, contact_id, flow_id, org_id, session_id, start_id, parent_uuid, connection_id)
				VALUES($1, $2, NOW(), NOW(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
				RETURNING id
			`,
		r.UUID, r.IsActive, exitedOn, r.ExitType, r.Status, r.ExpiresOn, r.Responded, r.Results, r.Path, r.Events, nil, r.ContactID, r.FlowID, r.OrgID, nil, nil, nil, nil,
	)

	if err != nil {
		return errors.Wrapf(err, "error writing empty run")
	}

	return nil
}
