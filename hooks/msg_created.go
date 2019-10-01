package hooks

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/urns"

	"github.com/apex/log"
	"github.com/edganiukov/fcm"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterPreWriteHook(events.TypeMsgCreated, handlePreMsgCreated)
	models.RegisterEventHook(events.TypeMsgCreated, handleMsgCreated)
}

// SendMessagesHook is our hook for sending session messages
type SendMessagesHook struct{}

var sendMessagesHook = &SendMessagesHook{}

// Apply sends all non-android messages to courier
func (h *SendMessagesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// messages that need to be marked as pending
	pending := make([]*models.Msg, 0, 1)

	// android channels that need to be notified to sync
	androidChannels := make(map[*models.Channel]bool)

	// for each session gather all our messages
	for s, args := range sessions {
		// walk through our messages, separate by whether they have a topup
		courierMsgs := make([]*models.Msg, 0, len(args))

		for _, m := range args {
			msg := m.(*models.Msg)
			channel := msg.Channel()
			if msg.TopupID() != models.NilTopupID && channel != nil {
				if channel.Type() == models.ChannelTypeAndroid {
					androidChannels[channel] = true
				} else {
					courierMsgs = append(courierMsgs, msg)
				}
			} else {
				pending = append(pending, msg)
			}
		}

		// if there are courier messages to send, do so
		if len(courierMsgs) > 0 {
			// if our session has a timeout, set it on our last message
			if s.Timeout() != nil && s.WaitStartedOn() != nil {
				courierMsgs[len(courierMsgs)-1].SetTimeout(s.ID(), *s.WaitStartedOn(), *s.Timeout())
			}

			log := log.WithField("messages", courierMsgs).WithField("session", s.ID)

			err := courier.QueueMessages(rc, courierMsgs)

			// not being able to queue a message isn't the end of the world, log but don't return an error
			if err != nil {
				log.WithError(err).Error("error queuing message")

				// in the case of errors we do want to change the messages back to pending however so they
				// get queued later. (for the common case messages are only inserted and queued, without a status update)
				for _, msg := range courierMsgs {
					pending = append(pending, msg)
				}
			}
		}
	}

	// if we have any android messages, trigger syncs for the unique channels
	for channel := range androidChannels {
		// no FCM key for this rapidpro install? break out but log
		if config.Mailroom.FCMKey == "" {
			logrus.Error("cannot trigger sync for android channel, FCM Key unset")
			break
		}

		// no fcm id for this channel, noop, we can't trigger a sync
		fcmID := channel.ConfigValue(models.ChannelConfigFCMID, "")
		if fcmID == "" {
			continue
		}

		client, err := fcm.NewClient(config.Mailroom.FCMKey)
		if err != nil {
			logrus.WithError(err).Error("error initializing fcm client")
			continue
		}

		sync := &fcm.Message{
			Token:       fcmID,
			Priority:    "high",
			CollapseKey: "sync",
			Data: map[string]interface{}{
				"msg": "sync",
			},
		}

		start := time.Now()
		_, err = client.Send(sync)

		if err != nil {
			// log failures but continue, relayer will sync on its own
			logrus.WithError(err).WithField("channel_uuid", channel.UUID()).Error("error syncing channel")
		} else {
			logrus.WithField("elapsed", time.Since(start)).WithField("channel_uuid", channel.UUID()).Debug("android sync complete")
		}
	}

	// any messages that didn't get sent should be moved back to pending (they are queued at creation to save an
	// update in the common case)
	if len(pending) > 0 {
		err := models.MarkMessagesPending(ctx, tx, pending)
		if err != nil {
			log.WithError(err).Error("error marking message as pending")
		}
	}

	return nil
}

// CommitMessagesHook is our hook for comitting session messages
type CommitMessagesHook struct{}

var commitMessagesHook = &CommitMessagesHook{}

// Apply takes care of inserting all the messages in the passed in sessions assigning topups to them as needed.
func (h *CommitMessagesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	msgs := make([]*models.Msg, 0, len(sessions))
	for _, s := range sessions {
		for _, m := range s {
			msgs = append(msgs, m.(*models.Msg))
		}
	}

	// find the topup we will assign
	rc := rp.Get()
	topup, err := models.DecrementOrgCredits(ctx, tx, rc, org.OrgID(), len(msgs))
	rc.Close()
	if err != nil {
		return errors.Wrapf(err, "error finding active topup")
	}

	// if we have an active topup, assign it to our messages
	if topup != models.NilTopupID {
		for _, m := range msgs {
			m.SetTopup(topup)
		}
	}

	// insert all our messages
	err = models.InsertMessages(ctx, tx, msgs)
	if err != nil {
		return errors.Wrapf(err, "error writing messages")
	}

	return nil
}

// handleMsgCreated creates the db msg for the passed in event
func handleMsgCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("msg created event")

	// ignore events that don't have a channel or URN set
	// TODO: maybe we should create these messages in a failed state?
	if session.SessionType() == models.MessagingFlow && (event.Msg.URN() == urns.NilURN || event.Msg.Channel() == nil) {
		return nil
	}

	// messaging flows must have urn id set on them, assert that
	if session.SessionType() == models.MessagingFlow {
		urnInt := models.GetURNInt(event.Msg.URN(), "id")
		if urnInt == 0 {
			return errors.Errorf("attempt to create messaging message with 0 id URN")
		}
	}

	// get our channel
	var channel *models.Channel
	if event.Msg.Channel() != nil {
		channel = org.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	msg, err := models.NewOutgoingMsg(org.OrgID(), channel, session.ContactID(), event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	// set our reply to as well (will be noop in cases when there is no incoming message)
	msg.SetResponseTo(session.IncomingMsgID(), session.IncomingMsgExternalID())

	// register to have this message committed
	session.AddPreCommitEvent(commitMessagesHook, msg)

	// don't send messages for surveyor flows
	if session.SessionType() != models.SurveyorFlow {
		session.AddPostCommitEvent(sendMessagesHook, msg)
	}

	return nil
}

// handlePreMsgCreated clears our timeout on our session so that courier can send it when the message is sent, that will be set by courier when sent
func handlePreMsgCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	// we only clear timeouts on messaging flows
	if session.SessionType() != models.MessagingFlow {
		return nil
	}

	// get our channel
	var channel *models.Channel

	if event.Msg.Channel() != nil {
		channel = org.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	// no channel? this is a no-op
	if channel == nil {
		return nil
	}

	// android channels get normal timeouts
	if channel.Type() == models.ChannelTypeAndroid {
		return nil
	}

	// everybody else gets their timeout cleared, will be set by courier
	session.ClearTimeoutOn()

	return nil
}
