package twiml

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/greatnonprofits-nfp/goflow/flows/routers/waits"
	"github.com/greatnonprofits-nfp/goflow/flows/routers/waits/hints"
	"github.com/greatnonprofits-nfp/goflow/utils"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/config"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestResponseForSprint(t *testing.T) {
	// for tests it is more convenient to not have formatted output
	indentMarshal = false

	urn := urns.URN("tel:+12067799294")
	channelRef := assets.NewChannelReference(assets.ChannelUUID(uuids.New()), "Twilio Channel")

	resumeURL := "http://temba.io/resume?session=1"

	// set our attachment domain for testing
	config.Mailroom.AttachmentDomain = "mailroom.io"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	tcs := []struct {
		Events   []flows.Event
		Wait     flows.ActivatedWait
		Expected string
	}{
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "eng", ""))},
			nil,
			`<Response><Say language="en-US">hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "ben", ""))},
			nil,
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`<Response><Play>https://mailroom.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:https://temba.io/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			nil,
			`<Response><Play>https://temba.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{})),
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "goodbye", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{})),
			},
			nil,
			`<Response><Say>hello world</Say><Say>goodbye</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewFixedDigitsHint(1)),
			`<Response><Gather numDigits="1" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number, then press #", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewTerminatedDigitsHint("#")),
			`<Response><Gather finishOnKey="#" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number, then press #</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "say something", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{}))},
			waits.NewActivatedMsgWait(nil, hints.NewAudioHint()),
			`<Response><Say>say something</Say><Record action="http://temba.io/resume?session=1&amp;wait_type=record" maxLength="600"></Record><Redirect>http://temba.io/resume?session=1&amp;wait_type=record&amp;empty=true</Redirect></Response>`,
		},
	}

	for i, tc := range tcs {
		response, err := responseForSprint(urn, resumeURL, tc.Wait, tc.Events)
		assert.NoError(t, err, "%d: unexpected error")
		assert.Equal(t, xml.Header+tc.Expected, response, "%d: unexpected response", i)
	}
}

func TestResumeForRequest(t *testing.T) {
	ctx, db, _ := testsuite.Reset()

	tClient, err := getTestClient(ctx, db, t)
	assert.NoError(t, err)

	req, err := getTestRequest(`{}`, "")
	req.Form = url.Values{
		"wait_type": []string{"dial"},
		"DialCallStatus": []string{"busy"},
		"DialCallDuration": []string{"10"},
	}

	resume, err := tClient.ResumeForRequest(req)
	assert.NoError(t, err)
	assert.Equal(t, ivr.DialResume{Status: "busy", Duration: 10}, resume)

	req.Form = url.Values{
		"wait_type": []string{"record"},
		"RecordingUrl": []string{"example.com/chill-town"},
	}

	resume, err = tClient.ResumeForRequest(req)
	assert.NoError(t, err)
	assert.Equal(t, ivr.InputResume{Attachment: "audio/mp3:example.com/chill-town.mp3"}, resume)
}

func TestValidateRequestSignature(t *testing.T) {
	ctx, db, _ := testsuite.Reset()
	defer testsuite.Reset()

	tClient, err := getTestClient(ctx, db, t)
	postFormData := url.Values{"Digits": []string{"10"}}
	sig, err := twCalculateSignature("https://temba.io/", postFormData, "twillio")
	assert.NoError(t, err)

	req, err := getTestRequest(`{}`, "")
	assert.NoError(t, err)

	fmt.Println(string(sig))
	err = tClient.ValidateRequestSignature(req)
	assert.Error(t, err, "missing request signature header")

	req.Header.Set("X-Twilio-Signature", "wrong-signature")
	err = tClient.ValidateRequestSignature(req)
	assert.Error(t, err, "invalid request signature: wrong-signature")

	req.Header.Set("X-Twilio-Signature", string(sig))
	req.PostForm = postFormData
	err = tClient.ValidateRequestSignature(req)
	assert.NoError(t, err)
}

func getTestRequest(reqBody string, reqPath string) (*http.Request, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	reqURL := "https://temba.io" + reqPath
	body := strings.NewReader(reqBody)
	return httpx.NewRequest("GET", reqURL, body, headers)
}

func getTestClient(ctx context.Context, db *sqlx.DB, t *testing.T) (ivr.Client, error) {
	db.MustExec(`UPDATE channels_channel SET config = '{"account_sid": "twillio", "auth_token": "twillio"}' WHERE id = $1`, models.TwilioChannelID)
	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)
	channel := oa.ChannelByUUID(models.TwilioChannelUUID)
	assert.NotNil(t, channel)

	return NewClientFromChannel(http.DefaultClient, channel)
}
