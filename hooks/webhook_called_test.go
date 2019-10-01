package hooks

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/actions"
)

type HookHandler struct{}

func (h *HookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query()["unsub"] != nil {
		w.WriteHeader(410)
	} else {
		w.WriteHeader(200)
	}
}

func TestWebhookCalled(t *testing.T) {
	// add a few resthooks
	testsuite.DB().MustExec(`INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'foo', 1, NOW(), NOW(), 1, 1);`)
	testsuite.DB().MustExec(`INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'bar', 1, NOW(), NOW(), 1, 1);`)

	handler := &HookHandler{}
	server := httptest.NewServer(handler)

	// and a few targets
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), $1, 1, 1, 1);`, server.URL)
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), $1, 1, 1, 2);`, server.URL+"?unsub=1")
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), $1, 1, 1, 1);`, server.URL+"?unsub=1")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewCallResthookAction(newActionUUID(), "foo", "foo"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewCallResthookAction(newActionUUID(), "foo", "foo"),
					actions.NewCallWebhookAction(newActionUUID(), "GET", server.URL+"?unsub=1", nil, "", ""),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = FALSE",
					Args:  nil,
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE and resthook_id = $1",
					Args:  []interface{}{2},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE",
					Args:  nil,
					Count: 2,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 200",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 410",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 3,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookevent where org_id = $1",
					Args:  []interface{}{models.Org1},
					Count: 2,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
