package handlers

import (
	"context"
	"encoding/json"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/jmoiron/sqlx"
)


func init() {
	models.RegisterEventHandler(events.TypeFeedbackRequested, handleFeedbackRequested)
}


func handleFeedbackRequested(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.FeedbackRequestedEvent)
	
	// must be in a session
	if scene.Session() == nil {
		return errors.Errorf("cannot handle msg created event without session")
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
	}).Debug("feedback requested event")
		
	// messages in messaging flows must have urn id set on them, if not, go look it up
	if scene.Session().SessionType() == models.FlowTypeMessaging && event.FeedbackRequest.URN() != urns.NilURN {
		urn := event.FeedbackRequest.URN()
		if models.GetURNInt(urn, "id") == 0 {
			urn, err := models.GetOrCreateURN(ctx, tx, oa, scene.ContactID(), event.FeedbackRequest.URN())
			if err != nil {
				return errors.Wrapf(err, "unable to get or create URN: %s", event.FeedbackRequest.URN())
			}
			// update our Msg with our full URN
			event.FeedbackRequest.SetURN(urn)
		}
	}

	// get our channel
	var channel *models.Channel
	if event.FeedbackRequest.Channel() != nil {
		channel = oa.ChannelByUUID(event.FeedbackRequest.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.FeedbackRequest.Channel().UUID)
		}
	}

	run, _ := scene.Session().FindStep(e.StepUUID())
	flow, _ := oa.FlowByUUID(run.FlowReference().UUID)

	questions, err := json.Marshal(map[string]interface{}{
		"feedback_request": map[string]string{
			"comment_question": event.FeedbackRequest.CommentQuestion(),
			"star_rating_question": event.FeedbackRequest.StarRatingQuestion(),
		},
	})
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.FeedbackRequest.URN())
	}


	msgOut := flows.NewMsgOut(
		event.FeedbackRequest.URN(),
		event.FeedbackRequest.Channel(),
		string(questions),
		[]utils.Attachment{},
		[]string{},
		nil,
		flows.MsgTopicFeedback,
		"",
		flows.ShareableIconsConfig{},
	)

	msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, scene.Session(), flow.(*models.Flow), msgOut, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.FeedbackRequest.URN())
	}

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitMessagesHook, msg)

	// don't send messages for surveyor flows
	if scene.Session().SessionType() != models.FlowTypeSurveyor {
		scene.AppendToEventPostCommitHook(hooks.SendMessagesHook, msg)
	}

	return nil
}
