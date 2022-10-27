package models_test

import (
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLookupOrgByUUIDAndToken(t *testing.T) {
	apiToken := "some-token-for-cathy"
	userId := int64(2)
	permission := "Administrators"
	ctx, _, db, _ := testsuite.Get()
	defer testsuite.Reset(testsuite.ResetDB)
	addUserToken(db, userId, apiToken)

	orgRef, err := models.LookupOrgByUUIDAndToken(ctx, db, testdata.Org2.UUID, "", apiToken)
	assert.NoError(t, err)
	assert.Nil(t, orgRef)

	orgRef, err = models.LookupOrgByUUIDAndToken(ctx, db, testdata.Org2.UUID, permission, apiToken)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Org2.UUID, orgRef.UUID)
}

func addUserToken(db *sqlx.DB, userId int64, apiToken string) {
	db.MustExec("INSERT INTO api_apitoken(key, org_id, role_id, user_id, is_active, created) VALUES ($1, $2, $3, $4, TRUE, $5)",
		apiToken, 2, 8, userId, time.Now())
}
