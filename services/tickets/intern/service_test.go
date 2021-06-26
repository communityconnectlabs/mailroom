package intern_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/assets/static/types"
	"github.com/greatnonprofits-nfp/goflow/envs"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/test"
	"github.com/greatnonprofits-nfp/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	intern "github.com/nyaruka/mailroom/services/tickets/intern"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "internal"))

	svc, err := intern.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		nil,
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}

	ticket, err := svc.Open(session, "Need help", "Where are my cookies?", logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, &flows.Ticket{
		UUID:       flows.TicketUUID("e7187099-7d38-4f60-955c-325957214c42"),
		Ticketer:   ticketer.Reference(),
		Subject:    "Need help",
		Body:       "Where are my cookies?",
		ExternalID: "",
	}, ticket)

	assert.Equal(t, 0, len(logger.Logs))

	dbTicket := models.NewTicket(ticket.UUID, models.Org1, models.CathyID, models.InternalID, "", "Need help", "Where are my cookies?", nil)

	logger = &flows.HTTPLogger{}
	err = svc.Forward(
		dbTicket,
		flows.MsgUUID("ca5607f0-cba8-4c94-9cd5-c4fbc24aa767"),
		"It's urgent",
		[]utils.Attachment{utils.Attachment("image/jpg:http://myfiles.com/media/0123/attachment1.jpg")},
		logger.Log,
	)

	// forwarding is a NOOP for internal ticketers
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))
}

func TestCloseAndReopen(t *testing.T) {
	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "internal"))
	svc, err := intern.NewService(http.DefaultClient, nil, ticketer, nil)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}
	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", models.Org1, models.CathyID, models.InternalID, "12", "New ticket", "Where my cookies?", nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", models.Org1, models.BobID, models.InternalID, "14", "Second ticket", "Where my shoes?", nil)

	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)

	// NOOP
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)

	// NOOP
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))
}
