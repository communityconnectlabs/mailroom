package handlers

import (
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
)

func init() {
	models.RegisterEventHandler(events.TypeLookupCalled, handleWebhookCalled)
}
