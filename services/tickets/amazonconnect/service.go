package amazonconnect

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	typeAmazonConnect        = "amazonconnect"
	configurationEndpointURL = "endpoint_url"
)

var db *sqlx.DB
var lock = &sync.Mutex{}

func initDB(dbURL string) error {
	if db == nil {
		lock.Lock()
		defer lock.Unlock()
		newDB, err := sqlx.Open("postgres", dbURL)
		if err != nil {
			return errors.Wrapf(err, "unable to open database connection")
		}
		SetDB(newDB)
	}
	return nil
}

func SetDB(newDB *sqlx.DB) {
	db = newDB
}

func init() {
	models.RegisterTicketService(typeAmazonConnect, NewService)
}

type service struct {
	client   *Client
	ticketer *flows.Ticketer
	redactor utils.Redactor
}

// NewService creates a new Amazon Connect ticket service
func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	authToken := rtCfg.AmazonConnectAuthToken
	endpointURL := config[configurationEndpointURL]

	if authToken != "" && endpointURL != "" {
		if err := initDB(rtCfg.DB); err != nil {
			return nil, err
		}

		return &service{
			client:   NewClient(httpClient, httpRetries, authToken, endpointURL),
			ticketer: ticketer,
			redactor: utils.NewRedactor(flows.RedactionMask, authToken, endpointURL),
		}, nil
	}

	return nil, errors.New("missing auth_token or endpoint_url in amazon connect config")
}

// Open opens a ticket which for Amazon Connect means create a Chat Channel associated to a Chat User
func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticket := flows.OpenTicket(s.ticketer, topic, body, assignee)
	contact := session.Contact()

	// get messages for history
	after := session.Runs()[0].CreatedOn()
	cx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	msgs, err := models.SelectContactMessages(cx, db, int(contact.ID()), after)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get history messages")
	}

	// send history
	messages := ""
	for _, msg := range msgs {
		messages += fmt.Sprintf("%s\n", msg.Text())
	}

	m := &CreateChatMessageParams{
		Message:    messages,
		Timestamp:  time.Now().Format(time.RFC3339),
		Timezone:   "UTC",
		Identifier: contact.PreferredURN().URN().Path(),
		Ticket:     string(ticket.UUID()),
	}

	ticketMessage, trace, err := s.client.CreateMessage(m)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling Amazon Connect")
	}

	ticket.SetExternalID(ticketMessage.ContactID)
	return ticket, nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	contactIdentity := ticket.Config("contact-identity")

	if strings.TrimSpace(text) != "" {
		msg := &CreateChatMessageParams{
			Message:    text,
			Timestamp:  time.Now().Format(time.RFC3339),
			Timezone:   "UTC",
			Identifier: contactIdentity,
			Ticket:     string(ticket.UUID()),
		}
		_, trace, err := s.client.CreateMessage(msg)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling Amazon Connect")
		}
	}

	return nil
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return errors.New("Amazon Connect ticket type doesn't support reopening")
}
