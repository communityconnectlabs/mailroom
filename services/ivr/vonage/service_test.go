package vonage

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestResponseForSprint(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	rc := rp.Get()
	defer rc.Close()

	urn := urns.URN("tel:+12067799294")
	channelRef := assets.NewChannelReference(testdata.VonageChannel.UUID, "Vonage Channel")

	resumeURL := "http://temba.io/resume?session=1"

	// deactivate our twilio channel
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, testdata.VonageChannel.ID)

	// set our UUID generator
	uuids.SetGenerator(uuids.NewSeededGenerator(0))

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	channel := oa.ChannelByUUID(testdata.VonageChannel.UUID)
	assert.NotNil(t, channel)

	p, err := NewServiceFromChannel(http.DefaultClient, channel)
	assert.NoError(t, err)

	provider := p.(*service)

	indentMarshal = false

	tcs := []struct {
		Events   []flows.Event
		Wait     flows.ActivatedWait
		Expected string
	}{
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`[{"action":"talk","text":"hello world"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`[{"action":"stream","streamUrl":["/recordings/foo.wav"]}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:https://temba.io/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`[{"action":"stream","streamUrl":["https://temba.io/recordings/foo.wav"]}]`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{})),
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "goodbye", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{})),
			},
			nil,
			`[{"action":"talk","text":"hello world"},{"action":"talk","text":"goodbye"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewFixedDigitsHint(1)),
			`[{"action":"talk","text":"enter a number","bargeIn":true},{"action":"input","maxDigits":1,"submitOnHash":true,"timeOut":30,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=gather\u0026sig=OjsMUDhaBTUVLq1e6I4cM0SKYpk%3D"],"eventMethod":"POST"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number, then press #", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewTerminatedDigitsHint("#")),
			`[{"action":"talk","text":"enter a number, then press #","bargeIn":true},{"action":"input","maxDigits":20,"submitOnHash":true,"timeOut":30,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=gather\u0026sig=OjsMUDhaBTUVLq1e6I4cM0SKYpk%3D"],"eventMethod":"POST"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "say something", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewAudioHint()),
			`[{"action":"talk","text":"say something"},{"action":"record","endOnKey":"#","timeOut":600,"endOnSilence":5,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=recording_url\u0026recording_uuid=f3ede2d6-becc-4ea3-ae5e-88526a9f4a57\u0026sig=Am9z7fXyU3SPCZagkSpddZSi6xY%3D"],"eventMethod":"POST"},{"action":"input","submitOnHash":true,"timeOut":1,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=record\u0026recording_uuid=f3ede2d6-becc-4ea3-ae5e-88526a9f4a57\u0026sig=fX1RhjcJNN4xYaiojVYakaz5F%2Fk%3D"],"eventMethod":"POST"}]`,
		},
	}

	for i, tc := range tcs {
		response, err := provider.responseForSprint(ctx, rp, channel, nil, resumeURL, tc.Wait, tc.Events)
		assert.NoError(t, err, "%d: unexpected error")
		assert.Equal(t, tc.Expected, response, "%d: unexpected response", i)
	}
}

func TestCallIDForRequest(t *testing.T)  {
	ctx, _, db, _ := testsuite.Reset()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest(`{"noop": ""}`, "")
	assert.NoError(t, err)

	_, err = nClient.CallIDForRequest(req)
	assert.Error(t, err, "invalid json body")

	req, err = getTestRequest(`{"uuid": ""}`, "")
	assert.NoError(t, err)

	_, err = nClient.CallIDForRequest(req)
	assert.Error(t, err, "no uuid set on call")

	req, err = getTestRequest(`{"uuid": "test-uuid"}`, "")
	assert.NoError(t, err)

	callID, err := nClient.CallIDForRequest(req)
	assert.NoError(t, err)
	assert.Equal(t, "test-uuid", callID)
}

func TestURNForRequest(t *testing.T) {
	ctx, _, db, _ := testsuite.Reset()

	nClient, err := getNClient(ctx, db, t)
	inboundReq, err := getTestRequest(`{"from": "+23480678888", "direction": "inbound"}`, "")
	outboundReq, err := getTestRequest(`{"to": "+2348067111", "direction": "outbound"}`, "")

	urnInbound, err := nClient.URNForRequest(inboundReq)
	assert.NoError(t, err)

	urnOutbound, err := nClient.URNForRequest(outboundReq)
	assert.NoError(t, err)

	assert.Equal(t, urns.URN("tel:+23480678888"), urnInbound)
	assert.Equal(t, urns.URN("tel:+2348067111"), urnOutbound)
}

func TestResumeForRequest(t *testing.T) {
	ctx, _, db, _ := testsuite.Reset()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest(`{"dtmf": "6"}`, "?dial_status=no_answer&dial_duration=1")
	assert.NoError(t, err)

	requestFormValues := []string{"gather"}
	req.Form = url.Values{"wait_type": requestFormValues}
	resumeOutput, err := nClient.ResumeForRequest(req)
	assert.NoError(t, err)
	assert.Equal(t, ivr.InputResume{Input: "6", Attachment: ""}, resumeOutput)

	requestFormValues = []string{"resume"}
	req.Form = url.Values{"wait_type": requestFormValues}
	resumeOutput, err = nClient.ResumeForRequest(req)
	assert.Errorf(t, err, "unknown wait_type: resume")
	assert.Nil(t, resumeOutput)

	requestFormValues = []string{"dial"}
	req.Form = url.Values{"wait_type": requestFormValues}
	resumeOutput, err = nClient.ResumeForRequest(req)
	assert.NoError(t, err)
	assert.NotNil(t, resumeOutput)
	assert.Equal(t, ivr.DialResume{Status: "no_answer", Duration: 1}, resumeOutput)
}

func TestPreprocessStatus(t *testing.T) {
	ctx, _, db, rp := testsuite.Reset()
	rc := rp.Get()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest("", "")
	assert.NoError(t, err)

	assert.NoError(t, err)
	status, err := nClient.PreprocessStatus(ctx, db, rp, req)
	assert.NoError(t, err)
	assert.Nil(t, status)

	req, err = getTestRequest(`{"type": "transfer"}`, "")
	assert.NoError(t, err)
	status, err = nClient.PreprocessStatus(ctx, db, rp, req)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"_message":"ignoring conversation callback"}`), status)

	req, err = getTestRequest(`{"uuid": "test-neximo-uuid", "status": "busy"}`, "")
	assert.NoError(t, err)
	_, err = rc.Do("SET", "dial_test-neximo-uuid", "dial_uuid:active")
	status, err = nClient.PreprocessStatus(ctx, db, rp, req)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"_message":"updated status for call: dial_uuid to: busy"}`), status)

	redisValue, err := redis.String(rc.Do("get", "dial_status_dial_uuid"))
	assert.NoError(t, err)
	assert.Equal(t, "busy", redisValue)
}

func TestPreprocessResume(t *testing.T) {
	ctx, _, db, rp := testsuite.Reset()
	rc := rp.Get()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest("", "?wait_type=record&recording_uuid=1111-111-1111")
	assert.NoError(t, err)

	conn := &models.ChannelConnection{}
	resume, err := nClient.PreprocessResume(ctx, db, rp, conn, req)
	assert.NoError(t, err)
	expectedResume := []byte(`[{
		"action": "input",
		"submitOnHash": true,
		"timeOut": 1,
		"eventUrl": [
		  "https://temba.io/?wait_type=record\u0026recording_uuid=1111-111-1111"
		],
		"eventMethod": "POST"
	  }
	]`)
	assert.True(t, bytes.Compare(expectedResume, resume) == 1)

	_, err = rc.Do("SET", "recording_1111-111-1111", "https://example.com")
	assert.NoError(t, err)

	resume, err = nClient.PreprocessResume(ctx, db, rp, conn, req)
	assert.NoError(t, err)
	assert.Nil(t, resume)

	redisValue, err := redis.String(rc.Do("get", "recording_1111-111-1111"))
	assert.Equal(t, err, redis.ErrNil)
	assert.Equal(t, "", redisValue)

	req, err = getTestRequest(`{"recording_url": "https://example.com"}`, "?wait_type=recording_url&recording_uuid=1111-111-1111")
	assert.NoError(t, err)

	expectedResume = []byte(`{"_message": "inserted recording url: https://example.com for uuid: 1111-111-1111"}`)
	resume, err = nClient.PreprocessResume(ctx, db, rp, conn, req)
	assert.NoError(t, err)
	assert.True(t, bytes.Compare(expectedResume, resume) == 1)
}

func TestStatusForRequest(t *testing.T) {
	ctx, _, db, _ := testsuite.Reset()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest("", "")
	assert.NoError(t, err)

	_, statusInt := nClient.StatusForRequest(req)
	assert.Equal(t, 0, statusInt)


	_, statusInt = nClient.StatusForRequest(req)
	assert.Equal(t, 0, statusInt)
}

func TestValidateRequestSignature(t *testing.T)  {
	cl := client{appID: "app_id"}
	sig := cl.calculateSignature("https://temba.io/handle")

	ctx, _, db, _ := testsuite.Reset()
	defer testsuite.Reset()

	nClient, err := getNClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest("", "")
	assert.NoError(t, err)

	err = nClient.ValidateRequestSignature(req)
	assert.NoError(t, err)

	req, err = getTestRequest("", "/handle")
	err = nClient.ValidateRequestSignature(req)
	assert.Error(t, err, "missing request sig")

	req, err = getTestRequest("", "/handle?sig=test-sig")
	err = nClient.ValidateRequestSignature(req)
	assert.Errorf(t, err, "mismatch in signatures for url: https://temba.io/handle?sig=test-sig, actual: test-sig, expected: %s", sig)

	req, err = getTestRequest("", fmt.Sprintf("/handle?sig=%s", sig))
	err = nClient.ValidateRequestSignature(req)
	assert.NoError(t, err)
}

func getNClient(ctx context.Context, db *sqlx.DB, t *testing.T) (ivr.Client, error) {
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, testdata.VonageChannel.ID)

	oa, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	assert.NoError(t, err)
	channel := oa.ChannelByUUID(testdata.VonageChannel.UUID)
	assert.NotNil(t, channel)

	return NewClientFromChannel(http.DefaultClient, channel)
}

func getTestRequest(userReqBody string, reqPath string) (*http.Request, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	reqURL := "https://temba.io" + reqPath
	reqBody := `{
		"uuid": "tested-client-uuid",
		"direction": "inbound",
		"from": "+23480678888",
		"to": "",
		"status": "completed"
	}`
	if userReqBody != "" {
		reqBody = userReqBody
	}
	body := strings.NewReader(reqBody)
	return httpx.NewRequest("GET", reqURL, body, headers)
}
