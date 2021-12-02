package models_test

import (
	"context"
	"encoding/json"
	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/envs"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/engine"
	"github.com/greatnonprofits-nfp/goflow/flows/triggers"
	"github.com/greatnonprofits-nfp/goflow/test"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSession(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	txCTX, cancel := context.WithCancel(ctx)

	tx, err := db.BeginTxx(txCTX, nil)
	assert.NoError(t, err)

	defer func() {
		cancel()
		testsuite.Reset()
	}()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	assert.NoError(t, err)

	assetJSON, err := jsonx.Marshal(json.RawMessage(getSessionAssetsJSON()))
	assert.NoError(t, err)

	fs, sprint, err := createSession(assetJSON, models.FavoritesFlowUUID)
	assert.NoError(t, err)

	ss := []flows.Session{fs}
	sprints := []flows.Sprint{sprint}

	session, err := models.NewSession(ctx, tx, oa, fs, sprint)
	assert.NoError(t, err)

	assert.Equal(t, fs.UUID(), session.UUID())
	assert.Equal(t, models.SessionID(0), session.ID())

	sessions, err := models.WriteSessions(ctx, tx, rp, oa, ss, sprints, nil)
	assert.NoError(t, err)

	session = sessions[0]

	assert.Equal(t, 1, len(sessions))
	assert.Equal(t, models.SessionID(1), session.ID())

	sa, err := test.CreateSessionAssets(assetJSON, "")
	assert.NoError(t, err)

	err = session.WriteUpdatedSession(ctx, tx, rp, oa, fs, sprint, nil)
	assert.EqualError(t, err, "missing seen runs, cannot update session")

	var env envs.Environment
	fs, err = session.FlowSession(sa, env)
	assert.NoError(t, err)

	err = session.WriteUpdatedSession(ctx, tx, rp, oa, fs, sprint, nil)
	assert.NoError(t, err)
}

func TestNewEmptyRun(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	contactID := flows.ContactID(models.CathyID)
	flowID := models.FavoritesFlowID
	orgID := models.Org1
	runSQL := `SELECT COUNT(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2 AND org_id = $3`

	args := []interface{}{contactID, flowID, orgID}
	testsuite.AssertQueryCount(t, db, runSQL, args, 0, "mismatch in expected count for query: %s", runSQL)

	err := models.NewEmptyRun(ctx, db, contactID, flowID, orgID)
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, runSQL, args, 1, "mismatch in expected count for query: %s", runSQL)
}

func getSessionAssetsJSON() string {
	return `{
  "channels": [
    {
      "uuid": "57f1078f-88aa-46f4-a59a-948a5739c03d",
      "name": "My Android Phone",
      "address": "+17036975131",
      "schemes": [
        "tel"
      ],
      "roles": [
        "send",
        "receive"
      ],
      "country": "US"
    }
  ],
  "classifiers": [
    {
      "uuid": "1c06c884-39dd-4ce4-ad9f-9a01cbe6c000",
      "name": "Booking",
      "type": "wit",
      "intents": [
        "book_flight",
        "book_hotel"
      ]
    }
  ],
  "ticketers": [
    {
      "uuid": "19dc6346-9623-4fe4-be80-538d493ecdf5",
      "name": "Support Tickets",
      "type": "mailgun"
    }
  ],
  "flows": [
    {
      "id": 10000,
      "uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
      "name": "Favorites",
      "spec_version": "13.0",
      "language": "eng",
      "type": "messaging",
      "revision": 123,
      "nodes": [
        {
          "uuid": "72a1f5df-49f9-45df-94c9-d86f7ea064e5",
          "actions": [],
          "exits": [
            {
              "uuid": "d7a36118-0a38-4b35-a7e4-ae89042f0d3c"
            }
          ]
        }
      ]
    }
  ],
  "fields": [
    {
      "uuid": "d66a7823-eada-40e5-9a3a-57239d4690bf",
      "key": "gender",
      "name": "Gender",
      "type": "text"
    },
    {
      "uuid": "f1b5aea6-6586-41c7-9020-1a6326cc6565",
      "key": "age",
      "name": "Age",
      "type": "number"
    },
    {
      "uuid": "6c86d5ab-3fd9-4a5c-a5b6-48168b016747",
      "key": "join_date",
      "name": "Join Date",
      "type": "datetime"
    },
    {
      "uuid": "c88d2640-d124-438a-b666-5ec53a353dcd",
      "key": "activation_token",
      "name": "Activation Token",
      "type": "text"
    },
    {
      "uuid": "3bfc3908-a402-48ea-841c-b73b5ef3a254",
      "key": "not_set",
      "name": "Not set",
      "type": "text"
    }
  ],
  "groups": [
    {
      "uuid": "5e9d8fab-5e7e-4f51-b533-261af5dea70d",
      "name": "Testers"
    }
  ],
  "labels": [
    {
      "uuid": "3f65d88a-95dc-4140-9451-943e94e06fea",
      "name": "Spam"
    }
  ],
  "locations": [
    {
      "name": "Rwanda",
      "aliases": [
        "Ruanda"
      ],
      "children": [
        {
          "name": "Kigali City",
          "aliases": [
            "Kigali",
            "Kigari"
          ],
          "children": [
            {
              "name": "Gasabo",
              "children": [
                {
                  "name": "Gisozi"
                },
                {
                  "name": "Ndera"
                }
              ]
            },
            {
              "name": "Nyarugenge",
              "children": []
            }
          ]
        }
      ]
    }
  ],
  "resthooks": [
    {
      "slug": "new-registration",
      "subscribers": [
        "http://localhost/?cmd=success"
      ]
    }
  ]
}`
}

func createSession(assetsJSON json.RawMessage, flowUUID assets.FlowUUID) (flows.Session, flows.Sprint, error) {
	sa, err := test.CreateSessionAssets(assetsJSON, "")
	if err != nil {
		return nil, nil, err
	}

	flow, err := sa.Flows().Get(flowUUID)
	if err != nil {
		return nil, nil, err
	}

	env := envs.NewBuilder().Build()
	var urnList []urns.URN
	fields := map[string]*flows.Value{}
	contact, err := flows.NewContact(
		sa,
		models.BobUUID,
		flows.ContactID(models.BobID),
		"Bob",
		envs.NilLanguage,
		flows.ContactStatusActive,
		nil,
		dates.Now(),
		nil,
		urnList,
		nil,
		fields,
		nil,
		)
	trigger := triggers.NewBuilder(env, flow.Reference(), contact).Manual().Build()
	eng := engine.NewBuilder().Build()

	return eng.NewSession(sa, trigger)
}
