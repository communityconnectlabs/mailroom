package goflow

import (
	"sync"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/webhooks"
	"github.com/nyaruka/mailroom/config"

	"github.com/shopspring/decimal"
)

var eng, simulator flows.Engine
var engInit, simulatorInit sync.Once

var emailFactory engine.EmailServiceFactory
var classificationFactory engine.ClassificationServiceFactory
var ticketFactory engine.TicketServiceFactory
var airtimeFactory engine.AirtimeServiceFactory

// RegisterEmailServiceFactory can be used by outside callers to register a email factory
// for use by the engine
func RegisterEmailServiceFactory(factory engine.EmailServiceFactory) {
	emailFactory = factory
}

// RegisterClassificationServiceFactory can be used by outside callers to register a classification factory
// for use by the engine
func RegisterClassificationServiceFactory(factory engine.ClassificationServiceFactory) {
	classificationFactory = factory
}

// RegisterTicketServiceFactory can be used by outside callers to register a ticket service factory
// for use by the engine
func RegisterTicketServiceFactory(factory engine.TicketServiceFactory) {
	ticketFactory = factory
}

// RegisterAirtimeServiceFactory can be used by outside callers to register a airtime factory
// for use by the engine
func RegisterAirtimeServiceFactory(factory engine.AirtimeServiceFactory) {
	airtimeFactory = factory
}

// Engine returns the global engine instance for use with real sessions
func Engine(cfg *config.Config) flows.Engine {
	engInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + cfg.Version,
			"X-Mailroom-Mode": "normal",
		}

		httpClient, httpRetries, httpAccess := HTTP(cfg)

		eng = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, httpRetries, httpAccess, webhookHeaders, cfg.WebhooksMaxBodyBytes)).
			WithClassificationServiceFactory(classificationFactory).
			WithEmailServiceFactory(emailFactory).
			WithTicketServiceFactory(ticketFactory).
			WithAirtimeServiceFactory(airtimeFactory).
			WithMaxStepsPerSprint(cfg.MaxStepsPerSprint).
			Build()
	})

	return eng
}

// Simulator returns the global engine instance for use with simulated sessions
func Simulator(cfg *config.Config) flows.Engine {
	simulatorInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + cfg.Version,
			"X-Mailroom-Mode": "simulation",
		}

		httpClient, _, httpAccess := HTTP(cfg) // don't do retries in simulator

		simulator = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, nil, httpAccess, webhookHeaders, cfg.WebhooksMaxBodyBytes)).
			WithClassificationServiceFactory(classificationFactory).   // simulated sessions do real classification
			WithEmailServiceFactory(simulatorEmailServiceFactory).     // but faked emails
			WithTicketServiceFactory(simulatorTicketServiceFactory).   // and faked tickets
			WithAirtimeServiceFactory(simulatorAirtimeServiceFactory). // and faked airtime transfers
			WithMaxStepsPerSprint(cfg.MaxStepsPerSprint).
			Build()
	})

	return simulator
}

func simulatorEmailServiceFactory(session flows.Session) (flows.EmailService, error) {
	return &simulatorEmailService{}, nil
}

type simulatorEmailService struct{}

func (s *simulatorEmailService) Send(session flows.Session, addresses []string, subject, body string, attachments []string) error {
	return nil
}

func simulatorTicketServiceFactory(session flows.Session, ticketer *flows.Ticketer) (flows.TicketService, error) {
	return &simulatorTicketService{ticketer: ticketer}, nil
}

type simulatorTicketService struct {
	ticketer *flows.Ticketer
}

func (s *simulatorTicketService) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	return flows.OpenTicket(s.ticketer, subject, body), nil
}

func simulatorAirtimeServiceFactory(session flows.Session) (flows.AirtimeService, error) {
	return &simulatorAirtimeService{}, nil
}

type simulatorAirtimeService struct{}

func (s *simulatorAirtimeService) Transfer(session flows.Session, sender urns.URN, recipient urns.URN, amounts map[string]decimal.Decimal, logHTTP flows.HTTPLogCallback) (*flows.AirtimeTransfer, error) {
	transfer := &flows.AirtimeTransfer{
		Sender:        sender,
		Recipient:     recipient,
		DesiredAmount: decimal.Zero,
		ActualAmount:  decimal.Zero,
	}

	// pick arbitrary currency/amount pair in map
	for currency, amount := range amounts {
		transfer.Currency = currency
		transfer.DesiredAmount = amount
		transfer.ActualAmount = amount
		break
	}

	return transfer, nil
}
