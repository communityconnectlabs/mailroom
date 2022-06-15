package handlers

import (
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
)

func init() {
	models.RegisterEventHandler(events.TypeDialogflowCalled, handleWebhookCalled)
}
