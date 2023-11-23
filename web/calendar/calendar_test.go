package calendar_test

import (
	"fmt"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"testing"
)

func TestServer(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	db.MustExec(`UPDATE orgs_org SET config = '{"calendar_automation_flow": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85"}'::jsonb WHERE id = $1`, testdata.Org1.ID)

	web.RunWebTests(t, ctx, rt, "testdata/calendar_automation.json", map[string]string{
		"contact_uuid": fmt.Sprintf("%s", "d2f852ec-7b4e-457f-ae7f-f8b243c49ff5"),
	})
}
