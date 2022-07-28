package amazonconnect

import (
	"context"
	"net/http"
	"github.com/go-chi/chi"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/pkg/errors"
	"github.com/greatnonprofits-nfp/goflow/utils"
)

// https://mailroom.ccl.com/mr/tickets/types/amazonconnect/event_callback/123455-fasdf2323-fasdf

func init() {
	base := "/mr/tickets/types/amazonconnect"
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback/{ticket:[a-f0-9\\-]+}", web.WithHTTPLogs(handleEventCallback))
}

type eventCallbackRequest struct {
	EventType string `json:"event_type,omitempty"`
	AuthToken string `json:"auth_token,omitempty"`
	Text      string `json:"text,omitempty"`
}

func handleEventCallback(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	authToken := request.AuthToken
	if authToken != rt.Config.AmazonConnectAuthToken {
		return map[string]string{"status": "unauthorized"}, http.StatusUnauthorized, nil
	}

	ticketUUID := uuids.UUID(chi.URLParam(r, "ticket"))

	ticket, _, _, err := tickets.FromTicketUUID(ctx, rt.DB, flows.TicketUUID(ticketUUID), typeAmazonConnect)
	if err != nil {
		return errors.Errorf("no such ticket %s", ticketUUID), http.StatusNotFound, nil
	}

	switch request.EventType {
	case "agent-message":
		_, err = tickets.SendReply(ctx, rt, ticket, request.Text, []*tickets.File{})
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
	case "close-ticket":
		err = tickets.CloseTicket(ctx, rt, nil, ticket, false, l)

	default:
		err = errors.New("invalid event type")
	}
	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
