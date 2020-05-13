package dbrp

// The DBRP Mapping `Service` maps database, retention policy pairs to buckets.
// Every `DBRPMapping` stored is scoped to an organization ID.
// The service must ensure the following invariants are valid at any time:
//  - each orgID, database, retention policy triple must be unique;
//  - for each orgID and database there must exist one and only one default mapping (`mapping.Default` set to `true`).
// The service does so using three kv buckets:
//  - one for storing mappings;
//  - one for storing an index of mappings by orgID and database;
//  - one for storing the current default mapping for an orgID and a database.
//
// On *create*, the service creates the mapping.
// If another mapping with the same orgID, database, and retention policy exists, it fails.
// If the mapping is the first one for the specified orgID-database couple, it will be the default one.
//
// On *find*, the service find mappings.
// Every mapping returned uses the kv bucket where the default is specified to update the `mapping.Default` field.
//
// On *update*, the service updates the mapping.
// If the update causes another bucket to have the same orgID, database, and retention policy, it fails.
// If the update unsets `mapping.Default`, the first mapping found is set as default.
//
// On *delete*, the service updates the mapping.
// If the deletion deletes the default mapping, the first mapping found is set as default.
//
// NOTE: every *find* operation does not update default values for mappings in the transaction for retrieving the mapping.
// This should not cause any relevant inconsistency.
// *Write* operations on `Default` are consistent with writes.

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	bucket        = []byte("dbrpv1")
	indexBucket   = []byte("dbrpbyorganddbindexv1")
	defaultBucket = []byte("dbrpdefaultv1")
)

var _ influxdb.DBRPMappingServiceV2 = (*AuthorizedService)(nil)

type Service struct {
	store kv.Store
	IDGen influxdb.IDGenerator

	bucketSvc        influxdb.BucketService
	byOrgAndDatabase *kv.Index
}

func indexForeignKey(dbrp influxdb.DBRPMappingV2) []byte {
	return composeForeignKey(dbrp.OrganizationID, dbrp.Database)
}

func composeForeignKey(orgID influxdb.ID, db string) []byte {
	encID, _ := orgID.Encode()
	key := make([]byte, len(encID)+len(db))
	copy(key, encID)
	copy(key[len(encID):], db)
	return key
}

func NewService(ctx context.Context, bucketSvc influxdb.BucketService, st kv.Store) (influxdb.DBRPMappingServiceV2, error) {
	if err := st.Update(ctx, func(tx kv.Tx) error {
		_, err := tx.Bucket(bucket)
		if err != nil {
			return err
		}
		_, err = tx.Bucket(indexBucket)
		if err != nil {
			return err
		}
		_, err = tx.Bucket(defaultBucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &Service{
		store:     st,
		IDGen:     snowflake.NewDefaultIDGenerator(),
		bucketSvc: bucketSvc,
		byOrgAndDatabase: kv.NewIndex(kv.NewIndexMapping(bucket, indexBucket, func(v []byte) ([]byte, error) {
			var dbrp influxdb.DBRPMappingV2
			if err := json.Unmarshal(v, &dbrp); err != nil {
				return nil, err
			}
			return indexForeignKey(dbrp), nil
		}), kv.WithIndexReadPathEnabled),
	}, nil
}

// getDefault returns the default mapping ID for the given orgID and db as bytes.
func (s *Service) getDefault(ctx context.Context, orgID influxdb.ID, db string) ([]byte, error) {
	var id []byte
	err := s.store.View(ctx, func(tx kv.Tx) error {
		defID, err := s.getDefaultTx(tx, composeForeignKey(orgID, db))
		if err != nil {
			return err
		}
		id = defID
		return nil
	})
	return id, err
}

// getDefaultID returns the default mapping ID for the given orgID and db.
func (s *Service) getDefaultID(ctx context.Context, orgID influxdb.ID, db string) (influxdb.ID, error) {
	defID, err := s.getDefault(ctx, orgID, db)
	if err != nil {
		return 0, err
	}
	id := new(influxdb.ID)
	if err := id.Decode(defID); err != nil {
		return 0, err
	}
	return *id, nil
}

// isDefault tells whether a mapping is the default one.
func (s *Service) isDefault(ctx context.Context, m *influxdb.DBRPMappingV2) (bool, error) {
	defID, err := s.getDefault(ctx, m.OrganizationID, m.Database)
	if err != nil {
		if kv.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	dbrpID, _ := m.ID.Encode()
	return bytes.Equal(dbrpID, defID), nil
}

// updateDefault updates the default value of the given mapping based on the default value stored.
func (s *Service) updateDefault(ctx context.Context, m *influxdb.DBRPMappingV2) error {
	if d, err := s.isDefault(ctx, m); err != nil {
		return err
	} else if d {
		m.Default = true
	} else {
		m.Default = false
	}
	return nil
}

// isDBRPUnique verifies if the triple orgID-database-retention-policy is unique.
func (s *Service) isDBRPUnique(ctx context.Context, m influxdb.DBRPMappingV2) error {
	return s.store.View(ctx, func(tx kv.Tx) error {
		return s.byOrgAndDatabase.Walk(ctx, tx, composeForeignKey(m.OrganizationID, m.Database), func(k, v []byte) error {
			dbrp := &influxdb.DBRPMappingV2{}
			if err := json.Unmarshal(v, dbrp); err != nil {
				return ErrInternalService(err)
			}
			if dbrp.ID == m.ID {
				// Corner case.
				// This is the very same DBRP, just skip it!
				return nil
			}
			if dbrp.RetentionPolicy == m.RetentionPolicy {
				return ErrDBRPAlreadyExists("another DBRP mapping with same orgID, db, and rp exists")
			}
			return nil
		})
	})
}

// getDefaultTx gets the default mapping ID inside of a transaction.
func (s *Service) getDefaultTx(tx kv.Tx, compKey []byte) ([]byte, error) {
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return nil, err
	}
	defID, err := b.Get(compKey)
	if err != nil {
		return nil, err
	}
	return defID, nil
}

// setDefaultTx sets the given ID as default according to the given default value.
// setDefaultTx ensures that the first value for the given composite key is always the default,
// disrespectfully of the given default value.
// NOTE: updates to the bucket happen in a transaction.
func (s *Service) setDefaultTx(tx kv.Tx, compKey []byte, id []byte, defValue bool) (bool, error) {
	set := defValue
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return defValue, ErrInternalService(err)
	}
	_, err = b.Get(compKey)
	if err != nil {
		if kv.IsNotFound(err) {
			// This is the only one.
			// This has to be default, no matter defValue.
			set = true
		} else {
			return defValue, ErrInternalService(err)
		}
	}
	if set {
		if err := b.Put(compKey, id); err != nil {
			return defValue, ErrInternalService(err)
		}
	}
	return set, nil
}

// setFirstDefaultTx sets the first mapping for the given composite key as default.
// NOTE: updates to the bucket happen in a transaction.
func (s *Service) setFirstDefaultTx(tx kv.Tx, compKey []byte) error {
	next, err := s.byOrgAndDatabase.First(tx, compKey)
	if err != nil {
		if kv.IsNotFound(err) {
			// Nothing to do here, there is nothing to set as default.
			return nil
		}
		return ErrInternalService(err)
	}
	var m influxdb.DBRPMappingV2
	if err := json.Unmarshal(next, &m); err != nil {
		return ErrInternalService(err)
	}
	nextID, _ := m.ID.Encode()
	_, err = s.setDefaultTx(tx, compKey, nextID, true)
	return err
}

// FindBy returns the mapping for the given ID.
func (s *Service) FindByID(ctx context.Context, orgID, id influxdb.ID) (*influxdb.DBRPMappingV2, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidDBRPID
	}

	var b []byte
	if err := s.store.View(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		b, err = bucket.Get(encodedID)
		if err != nil {
			return ErrDBRPNotFound
		}
		return nil
	}); err != nil {
		return nil, err
	}

	dbrp := &influxdb.DBRPMappingV2{}
	if err := json.Unmarshal(b, dbrp); err != nil {
		return nil, ErrInternalService(err)
	}
	// If the given orgID is wrong, it is as if we did not found a mapping scoped to this org.
	if dbrp.OrganizationID != orgID {
		return nil, ErrDBRPNotFound
	}
	// Update the default value for this DBRP.
	if err := s.updateDefault(ctx, dbrp); err != nil {
		return nil, ErrInternalService(err)
	}
	return dbrp, nil
}

// FindMany returns a list of mappings that match filter and the total count of matching dbrp mappings.
// TODO(affo): find a smart way to apply FindOptions to a list of items.
func (s *Service) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	// Memoize defaults.
	defs := make(map[string]*influxdb.ID)
	get := func(orgID influxdb.ID, db string) (*influxdb.ID, error) {
		k := orgID.String() + db
		if _, ok := defs[k]; !ok {
			id, err := s.getDefaultID(ctx, orgID, db)
			if err != nil {
				if kv.IsNotFound(err) {
					defs[k] = nil
				} else {
					return nil, err
				}
			} else {
				defs[k] = &id
			}
		}
		return defs[k], nil
	}

	var err error
	ms := []*influxdb.DBRPMappingV2{}

	addDBRP := func(_, v []byte) error {
		m := &influxdb.DBRPMappingV2{}
		if err := json.Unmarshal(v, m); err != nil {
			return ErrInternalService(err)
		}
		// Updating the Default field must be done before filtering.
		defID, err := get(m.OrganizationID, m.Database)
		if err != nil {
			return ErrInternalService(err)
		}
		if m.ID == *defID {
			m.Default = true
		} else {
			m.Default = false
		}
		if filterFunc(m, filter) {
			ms = append(ms, m)
		}
		return nil
	}

	// Optimized path VS non-optimized one.
	if orgID, db := filter.OrgID, filter.Database; orgID != nil && db != nil {
		err = s.store.View(ctx, func(tx kv.Tx) error {
			return s.byOrgAndDatabase.Walk(ctx, tx, composeForeignKey(*orgID, *db), addDBRP)
		})
	} else {
		err = s.store.View(ctx, func(tx kv.Tx) error {
			bucket, err := tx.Bucket(bucket)
			if err != nil {
				return ErrInternalService(err)
			}
			cur, err := bucket.Cursor()
			if err != nil {
				return ErrInternalService(err)
			}

			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if err := addDBRP(k, v); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return ms, len(ms), err
}

// Create creates a new mapping.
// If another mapping with same organization ID, database, and retention policy exists, an error is returned.
// If the mapping already contains a valid ID, that one is used for storing the mapping.
func (s *Service) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if !dbrp.ID.Valid() {
		dbrp.ID = s.IDGen.ID()
	}
	if err := dbrp.Validate(); err != nil {
		return ErrInvalidDBRP(err)
	}

	if _, err := s.bucketSvc.FindBucketByID(ctx, dbrp.BucketID); err != nil {
		return err
	}

	// If a dbrp with this particular ID already exists an error is returned.
	if _, err := s.FindByID(ctx, dbrp.OrganizationID, dbrp.ID); err == nil {
		return ErrDBRPAlreadyExists("dbrp already exist for this particular ID. If you are trying an update use the right function .Update")
	}
	// If a dbrp with this orgID, db, and rp exists an error is returned.
	if err := s.isDBRPUnique(ctx, *dbrp); err != nil {
		return err
	}

	encodedID, err := dbrp.ID.Encode()
	if err != nil {
		return ErrInvalidDBRPID
	}
	b, err := json.Marshal(dbrp)
	if err != nil {
		return ErrInternalService(err)
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		if err := bucket.Put(encodedID, b); err != nil {
			return err
		}
		compKey := indexForeignKey(*dbrp)
		if err := s.byOrgAndDatabase.Insert(tx, compKey, encodedID); err != nil {
			return err
		}
		newDef, err := s.setDefaultTx(tx, compKey, encodedID, dbrp.Default)
		if err != nil {
			return ErrInternalService(err)
		}
		dbrp.Default = newDef
		return nil
	})
}

// Updates a mapping.
// If another mapping with same organization ID, database, and retention policy exists, an error is returned.
// Un-setting `Default` for a mapping will cause the first one to become the default.
func (s *Service) Update(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if err := dbrp.Validate(); err != nil {
		return ErrInvalidDBRP(err)
	}
	oldDBRP, err := s.FindByID(ctx, dbrp.OrganizationID, dbrp.ID)
	if err != nil {
		return ErrDBRPNotFound
	}
	// Overwrite fields that cannot change.
	dbrp.ID = oldDBRP.ID
	dbrp.OrganizationID = oldDBRP.OrganizationID
	dbrp.BucketID = oldDBRP.BucketID
	dbrp.Database = oldDBRP.Database

	// If a dbrp with this orgID, db, and rp exists an error is returned.
	if err := s.isDBRPUnique(ctx, *dbrp); err != nil {
		return err
	}

	encodedID, err := dbrp.ID.Encode()
	if err != nil {
		return ErrInternalService(err)
	}
	b, err := json.Marshal(dbrp)
	if err != nil {
		return ErrInternalService(err)
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		if err := bucket.Put(encodedID, b); err != nil {
			return err
		}
		compKey := indexForeignKey(*dbrp)
		if dbrp.Default {
			_, err = s.setDefaultTx(tx, compKey, encodedID, dbrp.Default)
		} else if oldDBRP.Default {
			// This means default was unset.
			// Need to find a new default.
			err = s.setFirstDefaultTx(tx, compKey)
		}
		return err
	})
}

// Delete removes a mapping.
// Deleting a mapping that does not exists is not an error.
// Deleting the default mapping will cause the first one (if any) to become the default.
func (s *Service) Delete(ctx context.Context, orgID, id influxdb.ID) error {
	dbrp, err := s.FindByID(ctx, orgID, id)
	if err != nil {
		return nil
	}
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInternalService(err)
	}
	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		compKey := indexForeignKey(*dbrp)
		if err := bucket.Delete(encodedID); err != nil {
			return err
		}
		if err := s.byOrgAndDatabase.Delete(tx, compKey, encodedID); err != nil {
			return ErrInternalService(err)
		}
		// If this was the default, we need to set a new default.
		if dbrp.Default {
			return s.setFirstDefaultTx(tx, compKey)
		}
		return nil
	})
}

// filterFunc is capable to validate if the dbrp is valid from a given filter.
// it runs true if the filtering data are contained in the dbrp.
func filterFunc(dbrp *influxdb.DBRPMappingV2, filter influxdb.DBRPMappingFilterV2) bool {
	return (filter.ID == nil || (*filter.ID) == dbrp.ID) &&
		(filter.OrgID == nil || (*filter.OrgID) == dbrp.OrganizationID) &&
		(filter.BucketID == nil || (*filter.BucketID) == dbrp.BucketID) &&
		(filter.Database == nil || (*filter.Database) == dbrp.Database) &&
		(filter.RetentionPolicy == nil || (*filter.RetentionPolicy) == dbrp.RetentionPolicy) &&
		(filter.Default == nil || (*filter.Default) == dbrp.Default)
}
