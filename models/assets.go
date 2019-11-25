package models

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/goflow"
	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// OrgAssets is our top level cache of all things contained in an org. It is used to build
// SessionAssets for the engine but also used to cache campaigns and other org level attributes
type OrgAssets struct {
	ctx     context.Context
	db      *sqlx.DB
	builtAt time.Time

	orgID OrgID

	env *Org

	flowByUUID map[assets.FlowUUID]assets.Flow

	flowByID      map[FlowID]assets.Flow
	flowCacheLock sync.RWMutex

	channels       []assets.Channel
	channelsByID   map[ChannelID]*Channel
	channelsByUUID map[assets.ChannelUUID]*Channel

	classifiers       []assets.Classifier
	classifiersByUUID map[assets.ClassifierUUID]*Classifier

	campaigns             []*Campaign
	campaignEventsByField map[FieldID][]*CampaignEvent
	campaignEventsByID    map[CampaignEventID]*CampaignEvent
	campaignsByGroup      map[GroupID][]*Campaign

	fields       []assets.Field
	fieldsByUUID map[assets.FieldUUID]*Field
	fieldsByKey  map[string]*Field

	groups       []assets.Group
	groupsByID   map[GroupID]*Group
	groupsByUUID map[assets.GroupUUID]*Group

	labels       []assets.Label
	labelsByUUID map[assets.LabelUUID]*Label

	resthooks []assets.Resthook
	templates []assets.Template
	triggers  []*Trigger
	globals   []assets.Global

	locations        []assets.LocationHierarchy
	locationsBuiltAt time.Time
}

var orgCache = cache.New(time.Hour, time.Minute*5)
var assetCache = cache.New(5*time.Second, time.Minute*5)
var ErrNotFound = errors.New("not found")

const cacheTimeout = time.Second * 5
const locationCacheTimeout = time.Hour

// FlushCache clears our entire org cache
func FlushCache() {
	orgCache.Flush()
	assetCache.Flush()
}

// NewOrgAssets creates and returns a new org assets objects, potentially using the previous
// org assets passed in to prevent refetching locations
func NewOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID, prev *OrgAssets) (*OrgAssets, error) {
	// build our new assets
	o := &OrgAssets{
		ctx:     ctx,
		db:      db,
		builtAt: time.Now(),

		orgID: orgID,

		channelsByID:   make(map[ChannelID]*Channel),
		channelsByUUID: make(map[assets.ChannelUUID]*Channel),

		classifiersByUUID: make(map[assets.ClassifierUUID]*Classifier),

		fieldsByUUID: make(map[assets.FieldUUID]*Field),
		fieldsByKey:  make(map[string]*Field),

		groupsByID:   make(map[GroupID]*Group),
		groupsByUUID: make(map[assets.GroupUUID]*Group),

		campaignEventsByField: make(map[FieldID][]*CampaignEvent),
		campaignEventsByID:    make(map[CampaignEventID]*CampaignEvent),
		campaignsByGroup:      make(map[GroupID][]*Campaign),

		labelsByUUID: make(map[assets.LabelUUID]*Label),

		flowByUUID: make(map[assets.FlowUUID]assets.Flow),
		flowByID:   make(map[FlowID]assets.Flow),
	}

	// we load everything at once except for flows which are lazily loaded
	var err error

	o.env, err = loadOrg(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading environment for org %d", orgID)
	}

	o.channels, err = loadChannels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading channel assets for org %d", orgID)
	}
	for _, c := range o.channels {
		channel := c.(*Channel)
		o.channelsByID[channel.ID()] = channel
		o.channelsByUUID[channel.UUID()] = channel
	}

	o.classifiers, err = loadClassifiers(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading classifier assets for org %d", orgID)
	}
	for _, c := range o.classifiers {
		o.classifiersByUUID[c.UUID()] = c.(*Classifier)
	}

	o.fields, err = loadFields(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading field assets for org %d", orgID)
	}
	for _, f := range o.fields {
		field := f.(*Field)
		o.fieldsByUUID[field.UUID()] = field
		o.fieldsByKey[field.Key()] = field
	}

	o.groups, err = loadGroups(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading group assets for org %d", orgID)
	}
	for _, g := range o.groups {
		group := g.(*Group)
		o.groupsByID[group.ID()] = group
		o.groupsByUUID[group.UUID()] = group
	}

	o.labels, err = loadLabels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading group labels for org %d", orgID)
	}
	for _, l := range o.labels {
		o.labelsByUUID[l.UUID()] = l.(*Label)
	}

	o.resthooks, err = loadResthooks(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading resthooks for org %d", orgID)
	}

	o.campaigns, err = loadCampaigns(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaigns for org %d", orgID)
	}
	for _, c := range o.campaigns {
		o.campaignsByGroup[c.GroupID()] = append(o.campaignsByGroup[c.GroupID()], c)
		for _, e := range c.Events() {
			o.campaignEventsByField[e.RelativeToID()] = append(o.campaignEventsByField[e.RelativeToID()], e)
			o.campaignEventsByID[e.ID()] = e
		}
	}

	o.triggers, err = loadTriggers(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading triggers for org %d", orgID)
	}

	o.templates, err = loadTemplates(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading templates for org %d", orgID)
	}

	o.globals, err = loadGlobals(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading globals for org %d", orgID)
	}

	// cache locations for an hour
	if prev != nil && time.Since(prev.locationsBuiltAt) < locationCacheTimeout {
		o.locations = prev.locations
		o.locationsBuiltAt = prev.locationsBuiltAt
	} else {
		o.locations, err = loadLocations(ctx, db, orgID)
		o.locationsBuiltAt = time.Now()
		if err != nil {
			return nil, errors.Wrapf(err, "error loading group locations for org %d", orgID)
		}
	}

	return o, nil
}

// GetOrgAssets creates or gets org assets for the passed in org
func GetOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) (*OrgAssets, error) {
	if db == nil {
		return nil, errors.Errorf("nil db, cannot load org")
	}

	// do we have a recent cache?
	key := fmt.Sprintf("%d", orgID)
	var cached *OrgAssets
	c, found := orgCache.Get(key)
	if found {
		cached = c.(*OrgAssets)
	}

	// if we found a source built in the last five seconds, use it
	if found && time.Since(cached.builtAt) < cacheTimeout {
		return cached, nil
	}

	// otherwise build a new one
	o, err := NewOrgAssets(ctx, db, orgID, cached)
	if err != nil {
		return nil, err
	}

	// add this org to our cache
	orgCache.Add(key, o, time.Minute)

	// return our assets
	return o, nil
}

// NewSessionAssets creates new sessions assets, returning the result
func NewSessionAssets(org *OrgAssets) (flows.SessionAssets, error) {
	return engine.NewSessionAssets(org, goflow.MigrationConfig())
}

// GetSessionAssets returns a goflow session assets object for the passed in org assets
func GetSessionAssets(org *OrgAssets) (flows.SessionAssets, error) {
	key := fmt.Sprintf("%d", org.OrgID())
	cached, found := assetCache.Get(key)
	if found {
		return cached.(flows.SessionAssets), nil
	}

	assets, err := NewSessionAssets(org)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating session assets from org")
	}

	assetCache.Set(key, assets, cache.DefaultExpiration)
	return assets, nil
}

func (a *OrgAssets) OrgID() OrgID { return a.orgID }

func (a *OrgAssets) Env() envs.Environment { return a.env }

func (a *OrgAssets) Org() *Org { return a.env }

func (a *OrgAssets) Channels() ([]assets.Channel, error) {
	return a.channels, nil
}

func (a *OrgAssets) ChannelByUUID(channelUUID assets.ChannelUUID) *Channel {
	return a.channelsByUUID[channelUUID]
}

func (a *OrgAssets) ChannelByID(channelID ChannelID) *Channel {
	return a.channelsByID[channelID]
}

// AddTestChannel adds a test channel to our org, this is only used in session assets during simulation
func (a *OrgAssets) AddTestChannel(channel assets.Channel) {
	a.channels = append(a.channels, channel)
	// we don't populate our maps for uuid or id, shouldn't be used in any hook anyways
}

func (a *OrgAssets) Classifiers() ([]assets.Classifier, error) {
	return a.classifiers, nil
}

func (a *OrgAssets) ClassifierByUUID(classifierUUID assets.ClassifierUUID) *Classifier {
	return a.classifiersByUUID[classifierUUID]
}

func (a *OrgAssets) Fields() ([]assets.Field, error) {
	return a.fields, nil
}

func (a *OrgAssets) FieldByUUID(fieldUUID assets.FieldUUID) *Field {
	return a.fieldsByUUID[fieldUUID]
}

func (a *OrgAssets) FieldByKey(key string) *Field {
	return a.fieldsByKey[key]
}

func (a *OrgAssets) Flow(flowUUID assets.FlowUUID) (assets.Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowByUUID[flowUUID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow, nil
	}

	dbFlow, err := loadFlowByUUID(a.ctx, a.db, a.orgID, flowUUID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %s", flowUUID)
	}

	if dbFlow == nil {
		return nil, ErrNotFound
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

func (a *OrgAssets) FlowByID(flowID FlowID) (*Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowByID[flowID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow.(*Flow), nil
	}

	dbFlow, err := loadFlowByID(a.ctx, a.db, a.orgID, flowID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %d", flowID)
	}

	if dbFlow == nil {
		return nil, ErrNotFound
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

// SetFlow sets the flow definition for the passed in ID. Should only be used for unit tests
func (a *OrgAssets) SetFlow(flowID FlowID, flow flows.Flow) (*Flow, error) {
	// build our definition
	definition, err := json.Marshal(flow)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling flow definition")
	}

	logrus.WithField("flow_id", flowID).WithField("flow_uuid", flow.UUID()).Debug("set debug flow")
	fmt.Println(string(definition))

	f := &Flow{}
	f.f.UUID = flow.UUID()
	f.f.Name = flow.Name()
	f.f.ID = flowID
	f.f.Definition = definition

	a.flowByID[flowID] = f
	a.flowByUUID[flow.UUID()] = f

	return f, nil
}

func (a *OrgAssets) Campaigns() []*Campaign {
	return a.campaigns
}

func (a *OrgAssets) CampaignByGroupID(groupID GroupID) []*Campaign {
	return a.campaignsByGroup[groupID]
}

func (a *OrgAssets) CampaignEventsByFieldID(fieldID FieldID) []*CampaignEvent {
	return a.campaignEventsByField[fieldID]
}

func (a *OrgAssets) CampaignEventByID(eventID CampaignEventID) *CampaignEvent {
	return a.campaignEventsByID[eventID]
}

func (a *OrgAssets) Groups() ([]assets.Group, error) {
	return a.groups, nil
}

func (a *OrgAssets) GroupByID(groupID GroupID) *Group {
	return a.groupsByID[groupID]
}

func (a *OrgAssets) GroupByUUID(groupUUID assets.GroupUUID) *Group {
	return a.groupsByUUID[groupUUID]
}

func (a *OrgAssets) Labels() ([]assets.Label, error) {
	return a.labels, nil
}

func (a *OrgAssets) LabelByUUID(uuid assets.LabelUUID) *Label {
	return a.labelsByUUID[uuid]
}

func (a *OrgAssets) Triggers() []*Trigger {
	return a.triggers
}

func (a *OrgAssets) Locations() ([]assets.LocationHierarchy, error) {
	return a.locations, nil
}

func (a *OrgAssets) Resthooks() ([]assets.Resthook, error) {
	return a.resthooks, nil
}

func (a *OrgAssets) ResthookBySlug(slug string) *Resthook {
	for _, r := range a.resthooks {
		if r.Slug() == slug {
			return r.(*Resthook)
		}
	}
	return nil
}

func (a *OrgAssets) Templates() ([]assets.Template, error) {
	return a.templates, nil
}

func (a *OrgAssets) Globals() ([]assets.Global, error) {
	return a.globals, nil
}
