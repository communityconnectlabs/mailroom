package models_test

import (
	"github.com/greatnonprofits-nfp/goflow/flows"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifiers(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshClassifiers)
	require.NoError(t, err)

	classifiers, err := oa.Classifiers()
	require.NoError(t, err)

	tcs := []struct {
		ID      models.ClassifierID
		UUID    assets.ClassifierUUID
		Name    string
		Intents []string
	}{
		{testdata.Luis.ID, testdata.Luis.UUID, "LUIS", []string{"book_flight", "book_car"}},
		{testdata.Wit.ID, testdata.Wit.UUID, "Wit.ai", []string{"register"}},
		{testdata.Bothub.ID, testdata.Bothub.UUID, "BotHub", []string{"intent"}},
	}

	assert.Equal(t, len(tcs), len(classifiers))
	for i, tc := range tcs {
		c := classifiers[i].(*models.Classifier)
		assert.Equal(t, tc.UUID, c.UUID())
		assert.Equal(t, tc.ID, c.ID())
		assert.Equal(t, tc.Name, c.Name())
		assert.Equal(t, tc.Intents, c.Intents())
	}

}

func TestClassifier_AsService(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	classifiers, err := loadClassifiers(ctx, db, 1)
	assert.NoError(t, err)
	classifier1 := classifiers[0]
	classifier := &flows.Classifier{Classifier: classifier1}

	c := &Classifier{}
	cc := &c.c
	cc.Type = "Fake"
	_, err = c.AsService(classifier)
	assert.EqualError(t, err, "unknown classifier type 'Fake' for classifier: ")

	c = classifier1.(*Classifier)
	classifierService, err := c.AsService(classifier)
	assert.NoError(t, err)
	_, ok := classifierService.(flows.ClassificationService)

	assert.True(t, ok)
}
