package calendar

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/sirupsen/logrus"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/calendar/trigger", web.RequireAuthToken(handleCalendarAutomation))
}

// Request to receive a calendar automation trigger.
type calendarAutomationRequest struct {
	OrgID     models.OrgID `json:"orgId"   validate:"required"`
	Id        string       `json:"id"`
	Subject   string       `json:"subject"`
	StartTime struct {
		DateTime string `json:"dateTime"`
		TimeZone string `json:"timeZone"`
	} `json:"start_time"`
	EndTime struct {
		DateTime string `json:"dateTime"`
		TimeZone string `json:"timeZone"`
	} `json:"end_time"`
	Attendees []struct {
		Type   string `json:"type"`
		Status struct {
			Response string    `json:"response"`
			Time     time.Time `json:"time"`
		} `json:"status"`
		EmailAddress struct {
			Name    string `json:"name"`
			Address string `json:"address"`
		} `json:"emailAddress"`
	} `json:"attendees"`
	Location struct {
		DisplayName  string `json:"displayName"`
		LocationType string `json:"locationType"`
		UniqueId     string `json:"uniqueId"`
		UniqueIdType string `json:"uniqueIdType"`
	} `json:"location"`
	Organizer struct {
		EmailAddress struct {
			Name    string `json:"name"`
			Address string `json:"address"`
		} `json:"emailAddress"`
	} `json:"organizer"`
	OnlineMeeting struct {
		JoinUrl string `json:"joinUrl"`
	} `json:"onlineMeeting"`
}

// handles a request to resend the given messages
func handleCalendarAutomation(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &calendarAutomationRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	automationFlow := oa.Org().ConfigValue("calendar_automation_flow", "")

	flow, err := models.LoadFlowByUUID(ctx, rt.DB, oa.OrgID(), assets.FlowUUID(automationFlow))
	if err != nil {
		return errors.Wrapf(err, "error selecting flow %s on organization %d", automationFlow, oa.OrgID()), http.StatusInternalServerError, nil
	}
	log := logrus.WithField("flow_name", flow.Name()).WithField("flow_uuid", flow.UUID())

	var attendeeEmail string
	var attendeeName string
	if len(request.Attendees) > 0 {
		attendeeEmail = request.Attendees[0].EmailAddress.Address
		attendeeName = request.Attendees[0].EmailAddress.Name
	}
	contactURN := fmt.Sprintf("tel:%s", request.Location.UniqueId)
	organizerEmail := request.Organizer.EmailAddress.Address
	subject := request.Subject

	contact, _, _, err := models.GetOrCreateContact(ctx, rt.DB, oa, []urns.URN{urns.URN(contactURN)}, models.NilChannelID)
	if err != nil {
		return errors.Wrapf(err, "error creating contact %s on organization %d", contactURN, oa.OrgID()), http.StatusInternalServerError, nil
	}
	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error converting the contact %s to a FlowContact on organization %d", contactURN, oa.OrgID()), http.StatusInternalServerError, nil
	}

	var params *types.XObject
	paramsMap := map[string]string{
		"organizer_email":  organizerEmail,
		"attendee_email":   attendeeEmail,
		"attendee_name":    attendeeName,
		"calendar_subject": subject,
		"start_date":       request.StartTime.DateTime,
		"end_date":         request.EndTime.DateTime,
		"event_id":         request.Id,
	}
	asJSON, err := json.Marshal(paramsMap)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal extra organization %d", oa.OrgID()), http.StatusInternalServerError, nil
	}
	log.WithField("params", paramsMap).Info("flow engine start for ", contactURN)
	params, err = types.ReadXObject(asJSON)

	// build our flow trigger
	flowTrigger := triggers.NewBuilder(oa.Env(), flow.Reference(), flowContact).Manual().WithParams(params).Build()

	_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{contact}, []flows.Trigger{flowTrigger}, nil, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow for contact"), http.StatusInternalServerError, nil
	}

	return map[string]interface{}{"contact_uuid": contact.UUID()}, http.StatusOK, nil
}
