package models_test

import (
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannels(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// add some tel specific config to channel 2
	db.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"], "allow_international": true}' WHERE id = $1`, testdata.VonageChannel.ID)

	// make twitter channel have a parent of twilio channel
	db.MustExec(`UPDATE channels_channel SET parent_id = $1 WHERE id = $2`, testdata.TwilioChannel.ID, testdata.TwitterChannel.ID)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, 1, models.RefreshChannels)
	require.NoError(t, err)

	channels, err := oa.Channels()
	require.NoError(t, err)

	tcs := []struct {
		ID                 models.ChannelID
		UUID               assets.ChannelUUID
		Name               string
		Address            string
		Schemes            []string
		Roles              []assets.ChannelRole
		Prefixes           []string
		AllowInternational bool
		Parent             *assets.ChannelReference
	}{
		{
			testdata.TwilioChannel.ID,
			testdata.TwilioChannel.UUID,
			"Twilio",
			"+13605551212",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive", "call", "answer"},
			nil,
			false,
			nil,
		},
		{
			testdata.VonageChannel.ID,
			testdata.VonageChannel.UUID,
			"Vonage",
			"5789",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive"},
			[]string{"250", "251"},
			true,
			nil,
		},
		{
			testdata.TwitterChannel.ID,
			testdata.TwitterChannel.UUID,
			"Twitter",
			"ureport",
			[]string{"twitter"},
			[]assets.ChannelRole{"send", "receive"},
			nil,
			false,
			assets.NewChannelReference(testdata.TwilioChannel.UUID, "Twilio"),
		},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		channel := channels[i].(*models.Channel)
		assert.Equal(t, tc.UUID, channel.UUID())
		assert.Equal(t, tc.ID, channel.ID())
		assert.Equal(t, tc.Name, channel.Name())
		assert.Equal(t, tc.Address, channel.Address())
		assert.Equal(t, tc.Roles, channel.Roles())
		assert.Equal(t, tc.Schemes, channel.Schemes())
		assert.Equal(t, tc.Prefixes, channel.MatchPrefixes())
		assert.Equal(t, tc.AllowInternational, channel.AllowInternational())
		assert.Equal(t, tc.Parent, channel.Parent())
	}
}

func TestConfigValue(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset()

	// add some tel specific config to channel 2
	db.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"], "allow_international": true, "delay": 1.5, "user_value": "vonage"}' WHERE id = $1`, testdata.VonageChannel.ID)
	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, 1, models.RefreshChannels)
	require.NoError(t, err)

	channel := oa.ChannelByID(testdata.VonageChannel.ID)

	assert.Equal(t, "default", channel.ConfigValue("not_found", "default"))
	assert.Equal(t, "true", channel.ConfigValue("allow_international", "false"))
	assert.Equal(t, "2", channel.ConfigValue("delay", "0"))
	assert.Equal(t, "vonage", channel.ConfigValue("user_value", ""))
}

func TestOrgIDForChannelUUID(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	notAChannelUUID := testdata.Bob.UUID
	orgID, err := models.OrgIDForChannelUUID(ctx, db, assets.ChannelUUID(notAChannelUUID))
	require.Errorf(t, err, "no channel found with uuid: %s", notAChannelUUID)
	assert.Equal(t, orgID, models.NilOrgID)

	orgID, err = models.OrgIDForChannelUUID(ctx, db, testdata.VonageChannel.UUID)
	require.NoError(t, err)
	assert.Equal(t, orgID, testdata.Org1.ID)
}
