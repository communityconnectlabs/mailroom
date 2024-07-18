package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	openapi "github.com/twilio/twilio-go/rest/lookups/v1"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"

	"github.com/twilio/twilio-go"
)

// ContactImportID is the type for contact import IDs
type ContactImportID null.Int

func (i ContactImportID) MarshalJSON() ([]byte, error)  { return null.Int(i).MarshalJSON() }
func (i *ContactImportID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, (*null.Int)(i)) }
func (i ContactImportID) Value() (driver.Value, error)  { return null.Int(i).Value() }
func (i *ContactImportID) Scan(value interface{}) error { return null.ScanInt(value, (*null.Int)(i)) }

// ContactImportBatchID is the type for contact import batch IDs
type ContactImportBatchID int64

// ContactImportStatus is the status of an import
type ContactImportStatus string

type MaxCarrierGroupCount int

type CarrierType string

// import status constants
const (
	ContactImportStatusPending    ContactImportStatus = "P"
	ContactImportStatusProcessing ContactImportStatus = "O"
	ContactImportStatusComplete   ContactImportStatus = "C"
	ContactImportStatusFailed     ContactImportStatus = "F"
	MobileCarrierType             CarrierType         = "mobile"
	VOIPCarrierType               CarrierType         = "voip"
)

type ContactImport struct {
	ID          ContactImportID     `db:"id"`
	OrgID       OrgID               `db:"org_id"`
	Status      ContactImportStatus `db:"status"`
	CreatedByID UserID              `db:"created_by_id"`
	FinishedOn  *time.Time          `db:"finished_on"`

	// we fetch unique batch statuses concatenated as a string, see https://github.com/jmoiron/sqlx/issues/168
	BatchStatuses string `db:"batch_statuses"`
}

var sqlLoadContactImport = `
         SELECT i.id, i.org_id, i.status, i.created_by_id, i.finished_on, array_to_string(array_agg(DISTINCT b.status), '') AS "batch_statuses"
           FROM contacts_contactimport i
LEFT OUTER JOIN contacts_contactimportbatch b ON b.contact_import_id = i.id
          WHERE i.id = $1
       GROUP BY i.id`

// LoadContactImport loads a contact import by ID
func LoadContactImport(ctx context.Context, db Queryer, id ContactImportID) (*ContactImport, error) {
	i := &ContactImport{}
	err := db.GetContext(ctx, i, sqlLoadContactImport, id)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contact import id=%d", id)
	}
	return i, nil
}

var sqlMarkContactImportFinished = `
UPDATE contacts_contactimport
   SET status = $2, finished_on = $3
 WHERE id = $1`

func (i *ContactImport) MarkFinished(ctx context.Context, db Queryer, status ContactImportStatus) error {
	now := dates.Now()
	i.Status = status
	i.FinishedOn = &now

	_, err := db.ExecContext(ctx, sqlMarkContactImportFinished, i.ID, i.Status, i.FinishedOn)
	return errors.Wrap(err, "error marking import as finished")
}

// ContactImportBatch is a batch of contacts within a larger import
type ContactImportBatch struct {
	ID       ContactImportBatchID `db:"id"`
	ImportID ContactImportID      `db:"contact_import_id"`
	Status   ContactImportStatus  `db:"status"`
	Specs    json.RawMessage      `db:"specs"`

	// the range of records from the entire import contained in this batch
	RecordStart int `db:"record_start"`
	RecordEnd   int `db:"record_end"`

	// results written after processing this batch
	NumCreated    int             `db:"num_created"`
	NumUpdated    int             `db:"num_updated"`
	NumBlocked    int             `db:"num_blocked"`
	NumErrored    int             `db:"num_errored"`
	BlockedUUIDs  json.RawMessage `db:"blocked_uuids"`
	Errors        json.RawMessage `db:"errors"`
	FinishedOn    *time.Time      `db:"finished_on"`
	CarrierGroups json.RawMessage `db:"carrier_groups"`
}

// Import does the actual import of this batch
func (b *ContactImportBatch) Import(ctx context.Context, rt *runtime.Runtime, orgID OrgID) error {
	// if any error occurs this batch should be marked as failed
	if err := b.tryImport(ctx, rt, orgID); err != nil {
		b.markFailed(ctx, rt.DB)
		return err
	}
	return nil
}

// holds work data for import of a single contact
type importContact struct {
	record      int
	spec        *ContactSpec
	contact     *Contact
	created     bool
	flowContact *flows.Contact
	mods        []flows.Modifier
	errors      []string
	carrierType CarrierType
}

func (b *ContactImportBatch) tryImport(ctx context.Context, rt *runtime.Runtime, orgID OrgID) error {
	if err := b.markProcessing(ctx, rt.DB); err != nil {
		return errors.Wrap(err, "error marking as processing")
	}

	// grab our org assets
	oa, err := GetOrgAssetsWithRefresh(ctx, rt, orgID, RefreshFields|RefreshGroups)
	if err != nil {
		return errors.Wrap(err, "error loading org assets")
	}

	// unmarshal this batch's specs
	var specs []*ContactSpec
	if err := jsonx.Unmarshal(b.Specs, &specs); err != nil {
		return errors.Wrap(err, "error unmarsaling specs")
	}

	// create our work data for each contact being created or updated
	imports := make([]*importContact, len(specs))
	for i := range imports {
		imports[i] = &importContact{record: b.RecordStart + i, spec: specs[i]}
	}

	if err := b.getOrCreateContacts(ctx, rt.DB, oa, imports); err != nil {
		return errors.Wrap(err, "error getting and creating contacts")
	}

	// gather up contacts and modifiers
	modifiersByContact := make(map[*flows.Contact][]flows.Modifier, len(imports))
	for _, imp := range imports {
		// ignore errored imports which couldn't get/create a contact
		if imp.contact != nil {
			modifiersByContact[imp.flowContact] = imp.mods
		}
	}

	// and apply in bulk
	_, err = ApplyModifiers(ctx, rt, oa, modifiersByContact)
	if err != nil {
		return errors.Wrap(err, "error applying modifiers")
	}

	if err := b.markComplete(ctx, rt.DB, imports); err != nil {
		return errors.Wrap(err, "unable to mark as complete")
	}

	return nil
}

// for each import, fetches or creates the contact, creates the modifiers needed to set fields etc
func (b *ContactImportBatch) getOrCreateContacts(ctx context.Context, db QueryerWithTx, oa *OrgAssets, imports []*importContact) error {
	sa := oa.SessionAssets()

	// build map of UUIDs to contacts
	contactsByUUID, err := b.loadContactsByUUID(ctx, db, oa, imports)
	if err != nil {
		return errors.Wrap(err, "error loading contacts by UUID")
	}
	var twilioClient *twilio.RestClient
	var validateCarrier bool

	validateCarrier, err = checkValidateCarrier(ctx, db, b.ImportID)
	if err != nil {
		return errors.Wrap(err, "error checking urn carrier validation option")
	}

	if validateCarrier {
		twilioClient = initLookup(oa)
	}

	for _, imp := range imports {
		addModifier := func(m flows.Modifier) { imp.mods = append(imp.mods, m) }
		addError := func(s string, args ...interface{}) { imp.errors = append(imp.errors, fmt.Sprintf(s, args...)) }
		spec := imp.spec
		var carrierInfo *PhoneNumberLookupOutput
		uuid := spec.UUID

		if len(spec.URNs) == 0 && !(oa.Org().RedactionPolicy() == "urns" || uuid != "") {
			addError("Missing any valid URNs; at least one should be provided or a Contact UUID")
			continue
		}

		if uuid != "" {
			imp.contact = contactsByUUID[uuid]
			if imp.contact == nil {
				addError("Unable to find contact with UUID '%s'", uuid)
				continue
			}

			imp.flowContact, err = imp.contact.FlowContact(oa)
			if err != nil {
				return errors.Wrapf(err, "error creating flow contact for %d", imp.contact.ID())
			}

		} else {
			var validatedURNs []urns.URN

			if validateCarrier {
				validationTestFn := getValidationFn(twilioClient)

				carrierInfo, validatedURNs, err = ValidateURNCarrier(*spec, validationTestFn)
				if err != nil {
					return errors.Wrap(err, "error validating urn carrier")
				}
				if len(validatedURNs) == 0 {
					addError("urn %s failed carrier validation", string(spec.URNs[0].Identity()))
					continue
				}
				spec.URNs = validatedURNs
				imp.carrierType = carrierInfo.CarrierType
			}

			imp.contact, imp.flowContact, imp.created, err = GetOrCreateContact(ctx, db, oa, spec.URNs, NilChannelID)
			if err != nil {
				urnStrs := make([]string, len(spec.URNs))
				for i := range spec.URNs {
					urnStrs[i] = string(spec.URNs[i].Identity())
				}

				addError("Unable to find or create contact with URNs %s", strings.Join(urnStrs, ", "))
				continue
			}
		}

		addModifier(modifiers.NewURNs(spec.URNs, modifiers.URNsAppend))

		if spec.Name != nil {
			addModifier(modifiers.NewName(*spec.Name))
		}
		if spec.Language != nil {
			lang, err := envs.ParseLanguage(*spec.Language)
			if err != nil {
				addError("'%s' is not a valid language code", *spec.Language)
			} else {
				addModifier(modifiers.NewLanguage(lang))
			}
		}

		for key, value := range spec.Fields {
			field := sa.Fields().Get(key)
			if field == nil {
				addError("'%s' is not a valid contact field key", key)
			} else {
				addModifier(modifiers.NewField(field, value))
			}
		}

		if validateCarrier {
			carrierTypeField := sa.Fields().Get("carrier_type")
			carrierNameField := sa.Fields().Get("carrier_name")
			if carrierTypeField != nil {
				addModifier(modifiers.NewField(carrierTypeField, string(carrierInfo.CarrierType)))
			}

			if carrierNameField != nil {
				addModifier(modifiers.NewField(carrierNameField, carrierInfo.CarrierName))
			}
		}

		if len(spec.Groups) > 0 {
			groups := make([]*flows.Group, 0, len(spec.Groups))
			for _, uuid := range spec.Groups {
				group := sa.Groups().Get(uuid)
				if group == nil {
					addError("'%s' is not a valid contact group UUID", uuid)
				} else {
					groups = append(groups, group)
				}
			}
			addModifier(modifiers.NewGroups(groups, modifiers.GroupsAdd))
		}
	}

	return nil
}

// loads any import contacts for which we have UUIDs
func (b *ContactImportBatch) loadContactsByUUID(ctx context.Context, db Queryer, oa *OrgAssets, imports []*importContact) (map[flows.ContactUUID]*Contact, error) {
	uuids := make([]flows.ContactUUID, 0, 50)
	for _, imp := range imports {
		if imp.spec.UUID != "" {
			uuids = append(uuids, imp.spec.UUID)
		}
	}

	// build map of UUIDs to contacts
	contacts, err := LoadContactsByUUID(ctx, db, oa, uuids)
	if err != nil {
		return nil, err
	}

	contactsByUUID := make(map[flows.ContactUUID]*Contact, len(contacts))
	for _, c := range contacts {
		contactsByUUID[c.UUID()] = c
	}
	return contactsByUUID, nil
}

func (b *ContactImportBatch) markProcessing(ctx context.Context, db Queryer) error {
	b.Status = ContactImportStatusProcessing
	_, err := db.ExecContext(ctx, `UPDATE contacts_contactimportbatch SET status = $2 WHERE id = $1`, b.ID, b.Status)
	return err
}

func (b *ContactImportBatch) markComplete(ctx context.Context, db Queryer, imports []*importContact) error {
	numCreated := 0
	numUpdated := 0
	numErrored := 0
	importErrors := make([]importError, 0, 10)
	blockedUUIDs := make([]flows.ContactUUID, 0)
	carrierGroups := map[CarrierType][]ContactID{}
	trackDuplicate := make(map[CarrierType]map[ContactID]bool)

	for _, imp := range imports {
		if imp.contact == nil {
			numErrored++
		} else if imp.created {
			numCreated++
		} else {
			numUpdated++
		}
		for _, e := range imp.errors {
			importErrors = append(importErrors, importError{Record: imp.record, Row: imp.spec.ImportRow, Message: e})
		}
		if imp.contact != nil && (imp.contact.Status() == ContactStatusBlocked) {
			blockedUUIDs = append(blockedUUIDs, imp.contact.UUID())
		}

		if imp.carrierType != "" {
			if trackDuplicate[imp.carrierType] == nil {
				trackDuplicate[imp.carrierType] = make(map[ContactID]bool)
			}
			if !trackDuplicate[imp.carrierType][imp.contact.ID()] {
				trackDuplicate[imp.carrierType][imp.contact.ID()] = true
				carrierGroups[imp.carrierType] = append(carrierGroups[imp.carrierType], imp.contact.ID())
			}
		}
	}

	errorsJSON, err := jsonx.Marshal(importErrors)
	if err != nil {
		return errors.Wrap(err, "error marshaling errors")
	}

	numBlocked := len(blockedUUIDs)
	blockedUUIDsJson, err := jsonx.Marshal(blockedUUIDs)
	if err != nil {
		return errors.Wrap(err, "error marshaling blocked contacts")
	}

	carrierGroupsJson, err := jsonx.Marshal(carrierGroups)

	if err != nil {
		return errors.Wrap(err, "error marshaling grouped contacts")
	}

	now := dates.Now()
	b.Status = ContactImportStatusComplete
	b.NumCreated = numCreated
	b.NumUpdated = numUpdated
	b.NumBlocked = numBlocked
	b.BlockedUUIDs = blockedUUIDsJson
	b.NumErrored = numErrored
	b.Errors = errorsJSON
	b.FinishedOn = &now
	b.CarrierGroups = carrierGroupsJson
	_, err = db.NamedExecContext(ctx,
		`UPDATE 
			contacts_contactimportbatch
		SET 
			status = :status, 
			num_created = :num_created, 
			num_updated = :num_updated, 
			num_blocked = :num_blocked,
			blocked_uuids = :blocked_uuids,
			num_errored = :num_errored, 
			errors = :errors, 
			carrier_groups = :carrier_groups,
			finished_on = :finished_on
		WHERE 
			id = :id`,
		b,
	)
	return err
}

func (b *ContactImportBatch) markFailed(ctx context.Context, db Queryer) error {
	now := dates.Now()
	b.Status = ContactImportStatusFailed
	b.FinishedOn = &now
	_, err := db.ExecContext(ctx, `UPDATE contacts_contactimportbatch SET status = $2, finished_on = $3 WHERE id = $1`, b.ID, b.Status, b.FinishedOn)
	return err
}

var loadContactImportBatchSQL = `
SELECT 
	id,
  	contact_import_id,
  	status,
  	specs,
  	record_start,
  	record_end
FROM
	contacts_contactimportbatch
WHERE
	id = $1`

// LoadContactImportBatch loads a contact import batch by ID
func LoadContactImportBatch(ctx context.Context, db Queryer, id ContactImportBatchID) (*ContactImportBatch, error) {
	b := &ContactImportBatch{}
	err := db.GetContext(ctx, b, loadContactImportBatchSQL, id)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ContactSpec describes a contact to be updated or created
type ContactSpec struct {
	UUID     flows.ContactUUID  `json:"uuid"`
	Name     *string            `json:"name"`
	Language *string            `json:"language"`
	URNs     []urns.URN         `json:"urns"`
	Fields   map[string]string  `json:"fields"`
	Groups   []assets.GroupUUID `json:"groups"`

	ImportRow int `json:"_import_row"`
}

// an error message associated with a particular record
type importError struct {
	Record  int    `json:"record"`
	Row     int    `json:"row"`
	Message string `json:"message"`
}

// Carrier validation functions here

var checkValidateCarrierValueSQL = `
SELECT validate_carrier
	FROM contacts_contactimport 
WHERE
	id = $1 AND is_active = TRUE AND validate_carrier = TRUE
`

type PhoneNumberLookupOutput struct {
	CarrierType CarrierType
	CarrierName string
	IsValid     bool
}

var getValidationFn = func(twilioClient *twilio.RestClient) FetchPhoneNumber {
	returnFn := func(PhoneNumber string, params *openapi.FetchPhoneNumberParams) (*openapi.LookupsV1PhoneNumber, error) {
		return twilioClient.LookupsV1.FetchPhoneNumber(PhoneNumber, params)
	}

	return returnFn
}

type FetchPhoneNumber func(string, *openapi.FetchPhoneNumberParams) (*openapi.LookupsV1PhoneNumber, error)

func checkValidateCarrier(ctx context.Context, db Queryer, id ContactImportID) (bool, error) {
	result, err := db.ExecContext(ctx, checkValidateCarrierValueSQL, id)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	rowCount := rows > 0
	return rowCount, nil
}

func getCarrierType(cType CarrierType) CarrierType {
	if cType == VOIPCarrierType || cType == MobileCarrierType {
		return MobileCarrierType
	}

	return cType
}

func initLookup(oa *OrgAssets) *twilio.RestClient {
	accountSid := oa.Org().ConfigValue("ACCOUNT_SID", "")
	authToken := oa.Org().ConfigValue("ACCOUNT_TOKEN", "")
	client := twilio.NewRestClientWithParams(twilio.RestClientParams{
		Username: accountSid,
		Password: authToken,
	})

	return client
}

func numberLookUp(fn FetchPhoneNumber, phoneNumber string) (*PhoneNumberLookupOutput, error) {
	types := []string{"carrier"}
	output := &PhoneNumberLookupOutput{}

	params := &openapi.FetchPhoneNumberParams{}
	params.SetType(types)
	resp, err := fn(phoneNumber, params)

	if err != nil {
		if !strings.Contains(err.Error(), "404") {
			return nil, err
		}
		// avoid nil pointer
		output.IsValid = false
		return output, nil
	}
	carrierInfo := *resp.Carrier

	if carrierInfo["type"] == nil || carrierInfo["name"] == nil || carrierInfo["type"] == "" || carrierInfo["name"] == "" {
		output.IsValid = false
		return output, nil
	}
	typeValue := CarrierType(fmt.Sprintf("%v", carrierInfo["type"]))
	output.CarrierName = fmt.Sprintf("%v", carrierInfo["name"])
	output.CarrierType = getCarrierType(typeValue)
	output.IsValid = true
	return output, nil
}

func ValidateURNCarrier(spec ContactSpec, fn FetchPhoneNumber) (*PhoneNumberLookupOutput, []urns.URN, error) {
	var validatedURNs []urns.URN
	var urn = spec.URNs[0]
	carrierInfo, err := numberLookUp(fn, fmt.Sprintf("%v", urn))
	if err != nil {
		return nil, validatedURNs, err
	}
	if carrierInfo.IsValid {
		validatedURNs = append(validatedURNs, urn)
	}

	return carrierInfo, validatedURNs, nil
}
