package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestChannelConnections(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer db.MustExec(`DELETE FROM channels_channelconnection`)

	conn, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	assert.NotEqual(t, models.ConnectionID(0), conn.ID())

	err = conn.UpdateExternalID(ctx, db, "test1")
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) from channels_channelconnection where external_id = 'test1' AND id = $1`, conn.ID()).Returns(1)

	conn2, err := models.SelectChannelConnection(ctx, db, conn.ID())
	assert.NoError(t, err)
	assert.Equal(t, "test1", conn2.ExternalID())
}

func TestInvalidChannelExternalID(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	wrongEID := "wrong_id"

	_, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	_, err = models.SelectChannelConnectionByExternalID(ctx, db, testdata.TwilioChannel.ID, "V", wrongEID)
	assert.Error(t, err, "unable to load channel connection with external id: %s", wrongEID)
}

func TestUpdateStatus(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	expectedEndTime := time.Now()
	expectedDuration1 := 5
	expectedDuration2 := 0

	conn, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)
	assert.Equal(t, conn.Status(), models.ConnectionStatusPending)

	err = conn.UpdateStatus(ctx, db, models.ConnectionStatusQueued, expectedDuration1, expectedEndTime)
	assert.NoError(t, err)

	assert.Equal(t, conn.Status(), models.ConnectionStatusQueued)

	err = conn.UpdateStatus(ctx, db, models.ConnectionStatusInProgress, expectedDuration2, time.Now())
	assert.NoError(t, err)
	assert.Equal(t, conn.Status(), models.ConnectionStatusInProgress)
}

func TestLoadChannelConnectionsToRetry(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	limit := 2

	yesterday := time.Now().AddDate(0, 0, -1)

	conn1, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.VonageChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusQueued, "")
	assert.NoError(t, err)

	_, err = models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.VonageChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	conns, err := models.LoadChannelConnectionsToRetry(ctx, db, limit)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conns))

	err = conn1.MarkThrottled(ctx, db, yesterday)
	assert.NoError(t, err)

	conns, err = models.LoadChannelConnectionsToRetry(ctx, db, limit)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conns))
}

func TestActiveChannelConnectionCount(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	conn1, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn2, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Bob.ID, testdata.Bob.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn3, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Alexandria.ID, testdata.Alexandria.URNID, models.ConnectionDirectionOut, models.ConnectionStatusWired, "")
	assert.NoError(t, err)

	count, err := models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	err = conn1.MarkThrottled(ctx, db, time.Now())
	assert.NoError(t, err)

	err = conn2.MarkBusy(ctx, db, time.Now())
	assert.NoError(t, err)

	count, err = models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	err = conn1.MarkStarted(ctx, db, time.Now())
	assert.NoError(t, err)

	retryWait := time.Second * time.Duration(1)
	err = conn2.MarkErrored(ctx, db, time.Now(), &retryWait, models.ConnectionErrorBusy)
	assert.NoError(t, err)

	count, err = models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	err = conn3.MarkFailed(ctx, db, time.Now())
	assert.NoError(t, err)

	count, err = models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestUpdateChannelConnectionStatuses(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	var connectionIDs []models.ConnectionID

	err := models.UpdateChannelConnectionStatuses(ctx, db, connectionIDs, models.ConnectionStatusInProgress)
	assert.NoError(t, err)

	conn1, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn2, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Bob.ID, testdata.Bob.URNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn3, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Alexandria.ID, testdata.Alexandria.URNID, models.ConnectionDirectionOut, models.ConnectionStatusInProgress, "")
	assert.NoError(t, err)

	count, err := models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	connectionIDs = append(connectionIDs, conn1.ID(), conn2.ID(), conn3.ID())

	err = models.UpdateChannelConnectionStatuses(ctx, db, connectionIDs, models.ConnectionStatusInProgress)
	assert.NoError(t, err)

	count, err = models.ActiveChannelConnectionCount(ctx, db, testdata.TwilioChannel.ID)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)
}
