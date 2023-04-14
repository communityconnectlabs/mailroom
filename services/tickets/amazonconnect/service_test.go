package amazonconnect_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/nyaruka/mailroom/services/tickets/amazonconnect"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets/static"
)

func TestOpenAndForward(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 0, time.UTC)))

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		endpointURL + "/connect-agent": {
			httpx.NewMockResponse(200, nil, `
				{
					"ticket": "12345",
					"contactId": "12345",
					"participantId": "12345",
					"userId": "12345",
					"datetime": "2022-09-20 00:20:00"
				}
			`),
			httpx.NewMockResponse(200, nil, `
				{
					"ticket": "12345",
					"contactId": "12345",
					"participantId": "12345",
					"userId": "12345",
					"datetime": "2022-09-20 00:20:00"
				}
			`),
		},
	}))

	mockDB, mock, err := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	dummyTime, _ := time.Parse(time.RFC1123, "2019-10-07T15:21:30")

	rows := sqlmock.NewRows([]string{"id", "uuid", "text", "high_priority", "created_on", "modified_on", "sent_on", "queued_on", "direction", "status", "visibility", "msg_type", "msg_count", "error_count", "next_attempt", "external_id", "attachments", "metadata", "broadcast_id", "channel_id", "contact_id", "contact_urn_id", "org_id", "topup_id"}).
		AddRow(100, "1348d654-e3dc-4f2f-add0-a9163dc48895", "Hi! I'll try to help you!", true, dummyTime, dummyTime, dummyTime, dummyTime, "O", "W", "V", "F", 1, 0, nil, "398", nil, nil, nil, 3, 2, 2, 3, 3).
		AddRow(101, "b9568e35-3a59-4f91-882f-fa021f591b13", "Where are you from?", true, dummyTime, dummyTime, dummyTime, dummyTime, "O", "W", "V", "F", 1, 0, nil, "399", nil, nil, nil, 3, 2, 2, 3, 3).
		AddRow(102, "c864c4e0-9863-4fd3-9f76-bee481b4a138", "I'm from Brazil", false, dummyTime, dummyTime, dummyTime, dummyTime, "I", "P", "V", "F", 1, 0, nil, "400", nil, nil, nil, 3, 2, 2, 3, nil)

	after, err := time.Parse("2006-01-02T15:04:05", "2019-10-07T15:21:30")
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT").
		WithArgs(1234567, after).
		WillReturnRows(rows)

	amazonconnect.SetDB(sqlxDB)

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "amazonconnect"))

	_, err = amazonconnect.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing auth_token or endpoint_url in amazon connect config")

	svc, err := amazonconnect.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"endpoint_url": endpointURL,
		},
	)
	assert.NoError(t, err)

	logger := &flows.HTTPLogger{}

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	defaultTopic := oa.SessionAssets().Topics().FindByName("General")

	_, err = svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)
	assert.NoError(t, err)

	logger = &flows.HTTPLogger{}
	_, err = svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)
	assert.Error(t, err)

	dbTicket := models.NewTicket("4fa340ae-1fb0-4666-98db-2177fe9bf31c", testdata.Org1.ID, testdata.Cathy.ID, testdata.Amazonconnect.ID, "", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
		"contact-uuid":       string(testdata.Cathy.UUID),
		"contact-display":    "Cathy",
		"contact-identifier": testdata.Cathy.URN.Path(),
	})
	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.NoError(t, err)
}
