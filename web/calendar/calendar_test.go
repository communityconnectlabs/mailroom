package calendar_test

import (
	"fmt"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"testing"
)

func TestServer(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	blake := testdata.InsertContact(db, testdata.Org1, "9eef59ef-21b3-4f51-a296-937529a30e38", "Blake", envs.NilLanguage, models.ContactStatusActive)

	web.RunWebTests(t, ctx, rt, "testdata/calendar_automation.json", map[string]string{
		"black_uuid": fmt.Sprintf("%s", blake.UUID),
	})
}
