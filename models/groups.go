package models

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// GroupID is our type for group ids
type GroupID int

// Group is our mailroom type for contact groups
type Group struct {
	g struct {
		ID    GroupID          `json:"id"`
		UUID  assets.GroupUUID `json:"uuid"`
		Name  string           `json:"name"`
		Query string           `json:"query"`
	}
}

// ID returns the ID for this group
func (g *Group) ID() GroupID { return g.g.ID }

// UUID returns the uuid for this group
func (g *Group) UUID() assets.GroupUUID { return g.g.UUID }

// Name returns the name for this group
func (g *Group) Name() string { return g.g.Name }

// Query returns the query string (if any) for this group
func (g *Group) Query() string { return g.g.Query }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Group, error) {
	start := time.Now()

	rows, err := db.Queryx(selectGroupsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying groups for org: %d", orgID)
	}
	defer rows.Close()

	groups := make([]assets.Group, 0, 10)
	for rows.Next() {
		group := &Group{}
		err = readJSONRow(rows, &group.g)
		if err != nil {
			return nil, errors.Wrap(err, "error reading group row")
		}

		groups = append(groups, group)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(groups)).Debug("loaded groups")

	return groups, nil
}

const selectGroupsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id, 
	uuid, 
	name, 
	query
FROM 
	contacts_contactgroup 
WHERE 
	org_id = $1 AND 
	is_active = TRUE AND
	group_type = 'U'
ORDER BY 
	name ASC
) r;
`

// RemoveContactsFromGroups fires a bulk SQL query to remove all the contacts in the passed in groups
func RemoveContactsFromGroups(ctx context.Context, tx Queryer, removals []*GroupRemove) error {
	if len(removals) == 0 {
		return nil
	}

	// convert to list of interfaces
	is := make([]interface{}, len(removals))
	for i := range removals {
		is[i] = removals[i]
	}
	return BulkSQL(ctx, "removing contacts from groups", tx, removeContactsFromGroupsSQL, is)
}

// GroupRemove is our struct to track group removals
type GroupRemove struct {
	ContactID ContactID `db:"contact_id"`
	GroupID   GroupID   `db:"group_id"`
}

const removeContactsFromGroupsSQL = `
DELETE FROM
	contacts_contactgroup_contacts
WHERE 
	id
IN (
	SELECT 
		c.id 
	FROM 
		contacts_contactgroup_contacts c,
		(VALUES(:contact_id, :group_id)) AS g(contact_id, group_id)
	WHERE
		c.contact_id = g.contact_id::int AND c.contactgroup_id = g.group_id::int
);
`

// AddContactsToGroups fires a bulk SQL query to remove all the contacts in the passed in groups
func AddContactsToGroups(ctx context.Context, tx Queryer, adds []*GroupAdd) error {
	if len(adds) == 0 {
		return nil
	}

	// convert to list of interfaces
	is := make([]interface{}, len(adds))
	for i := range adds {
		is[i] = adds[i]
	}
	return BulkSQL(ctx, "adding contacts to groups", tx, addContactsToGroupsSQL, is)
}

// GroupAdd is our struct to track a final group additions
type GroupAdd struct {
	ContactID ContactID `db:"contact_id"`
	GroupID   GroupID   `db:"group_id"`
}

const addContactsToGroupsSQL = `
INSERT INTO 
	contacts_contactgroup_contacts
	(contact_id, contactgroup_id)
VALUES(:contact_id, :group_id)
ON CONFLICT
	DO NOTHING
`

// ContactIDsForGroupIDs returns the unique contacts that are in the passed in groups
func ContactIDsForGroupIDs(ctx context.Context, tx Queryer, groupIDs []GroupID) ([]ContactID, error) {
	// now add all the ids for our groups
	rows, err := tx.QueryxContext(ctx, `SELECT DISTINCT(contact_id) FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(groupIDs))
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting contacts for groups")
	}
	defer rows.Close()

	contactIDs := make([]ContactID, 0, 10)
	var contactID ContactID
	for rows.Next() {
		err := rows.Scan(&contactID)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning contact id")
		}
		contactIDs = append(contactIDs, contactID)
	}

	return contactIDs, nil
}
