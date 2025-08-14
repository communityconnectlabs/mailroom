package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/msg/send", web.RequireAuthToken(handleSend))
}

// Request to send a message.
//
//	{
//	  "org_id": 1,
//	  "contact_id": 123456,
//	  "user_id": 56,
//	  "text": "hi there"
//	}
type sendRequest struct {
	OrgID       models.OrgID       `json:"org_id"       validate:"required"`
	UserID      models.UserID      `json:"user_id"      validate:"required"`
	ContactID   models.ContactID   `json:"contact_id"   validate:"required"`
	Text        string             `json:"text"`
	Attachments []utils.Attachment `json:"attachments"`
}

// handles a request to resend the given messages
func handleSend(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &sendRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	// load the contact and generate as a flow contact
	c, err := models.LoadContact(ctx, rt.DB, oa, request.ContactID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error loading contact")
	}

	contact, err := c.FlowContact(oa)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error creating flow contact")
	}

	out, ch := models.NewMsgOut(oa, contact, request.Text, request.Attachments, nil)
	var msg *models.Msg

	msg, err = models.NewOutgoingChatMsg(rt, oa.Org(), ch, models.ContactID(contact.ID()), out, dates.Now())

	if err != nil {
		return nil, 0, errors.Wrap(err, "error creating outgoing message")
	}

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
	if err != nil {
		return nil, 0, errors.Wrap(err, "error inserting outgoing message")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, []*models.Msg{msg})

	return map[string]interface{}{
		"id":          msg.ID(),
		"channel":     out.Channel(),
		"contact":     contact.Reference(),
		"urn":         msg.URN(),
		"text":        msg.Text(),
		"attachments": msg.Attachments(),
		"status":      msg.Status(),
		"created_on":  msg.CreatedOn(),
		"modified_on": msg.ModifiedOn(),
	}, http.StatusOK, nil
}
