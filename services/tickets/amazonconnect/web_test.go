package amazonconnect

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestEventCallback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	ticket := testdata.InsertOpenTicket(
		db,
		testdata.Org1,
		testdata.Cathy,
		testdata.Amazonconnect,
		"Need help",
		"Have you seen my cookies?",
		"12345",
		nil,
	)

	web.RunWebTests(t, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
