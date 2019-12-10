package hooks

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"

	"github.com/gomodule/redigo/redis"
	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/actions"
)

func TestMsgCreated(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	config.Mailroom.AttachmentDomain = "foo.bar.com"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	// add a URN for cathy so we can test all urn sends
	db.MustExec(
		`INSERT INTO contacts_contacturn(identity, path, scheme, priority, contact_id, org_id) 
		                          VALUES('tel:12065551212', '12065551212', 'tel', 10, $1, 1)`,
		models.CathyID)

	// delete all URNs for bob
	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, models.BobID)

	// change alexandrias URN to a twitter URN and set her language to eng so that a template gets used for her
	db.MustExec(`UPDATE contacts_contacturn SET identity = 'twitter:12345', path='12345', scheme='twitter' WHERE contact_id = $1`, models.AlexandriaID)
	db.MustExec(`UPDATE contacts_contact SET language='eng' WHERE id = $1`, models.AlexandriaID)

	msg1 := createIncomingMsg(db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start")

	templateAction := actions.NewSendMsgAction(newActionUUID(), "Template time", nil, nil, false)
	templateAction.Templating = &actions.Templating{
		Template:  &assets.TemplateReference{assets.TemplateUUID("9c22b594-fcab-4b29-9bcb-ce4404894a80"), "revive_issue"},
		Variables: []string{"@contact.name", "tooth"},
	}

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "Hello World", nil, []string{"yes", "no"}, true),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "Hello Attachments", []string{"image/png:/images/image1.png"}, nil, true),
				},
				models.BobID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "No URNs", nil, nil, false),
				},
				models.AlexandriaID: []flows.Action{
					templateAction,
				},
			},
			Msgs: ContactMsgMap{
				models.CathyID: msg1,
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello World' AND contact_id = $1 AND metadata = $2 AND response_to_id = $3 AND high_priority = TRUE",
					Args:  []interface{}{models.CathyID, `{"quick_replies":["yes","no"]}`, msg1.ID()},
					Count: 2,
				},
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello Attachments' AND contact_id = $1 AND attachments[1] = $2 AND status = 'Q' AND high_priority = FALSE",
					Args:  []interface{}{models.GeorgeID, "image/png:https://foo.bar.com/images/image1.png"},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id=$1;",
					Args:  []interface{}{models.BobID},
					Count: 0,
				},
				SQLAssertion{
					SQL: "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND text = $2 AND metadata = $3 AND direction = 'O' AND status = 'Q' AND channel_id = $4",
					Args: []interface{}{
						models.AlexandriaID,
						`Hi Alexandia, are you still experiencing problems with tooth?`,
						`{"templating":{"template":{"uuid":"9c22b594-fcab-4b29-9bcb-ce4404894a80","name":"revive_issue"},"language":"eng","variables":["Alexandia","tooth"]}}`,
						models.TwitterChannelID,
					},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)

	rc := testsuite.RP().Get()
	defer rc.Close()

	// Cathy should have 1 batch of queued messages at high priority
	count, err := redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/1", models.TwilioChannelUUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// One bulk for George
	count, err = redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/0", models.TwilioChannelUUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestNoTopup(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// no more credits
	db.MustExec(`UPDATE orgs_topup SET credits = 0 WHERE org_id = $1`, models.Org1)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "No Topup", nil, nil, false),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='No Topup' AND contact_id = $1 AND status = 'P'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
