package dbrp

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	bucket      = []byte("dbrpv1")
	indexBucket = []byte("dbrpbyorganddbindexv1")
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

// FindBy returns the dbrp mapping the for cluster, db and rp.
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
	// If the given orgID is wrong, it is as if we did not found a DBRP scoped to the org.
	if dbrp.OrganizationID != orgID {
		return nil, ErrDBRPNotFound
	}
	return dbrp, nil
}

// FindMany returns a list of dbrp mappings that match filter and the total count of matching dbrp mappings.
// TODO(affo): find a smart way to apply FindOptions to a list of items.
func (s *Service) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	dbrps := []*influxdb.DBRPMappingV2{}

	addDBRP := func(_, v []byte) error {
		dbrp := &influxdb.DBRPMappingV2{}
		if err := json.Unmarshal(v, dbrp); err != nil {
			return ErrInternalService(err)
		}
		if filterFunc(dbrp, filter) {
			dbrps = append(dbrps, dbrp)
		}
		return nil
	}

	if orgID, db := filter.OrgID, filter.Database; orgID != nil && db != nil {
		if err := s.store.View(ctx, func(tx kv.Tx) error {
			return s.byOrgAndDatabase.Walk(ctx, tx, composeForeignKey(*orgID, *db), addDBRP)
		}); err != nil {
			return nil, 0, err
		}
		return dbrps, len(dbrps), nil
	}

	if err := s.store.View(ctx, func(tx kv.Tx) error {
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
	}); err != nil {
		return nil, len(dbrps), err
	}
	return dbrps, len(dbrps), nil
}

func (s *Service) ensureOneDefault(ctx context.Context, tx kv.Tx, compKey []byte) error {
	bucket, err := tx.Bucket(bucket)
	if err != nil {
		return ErrInternalService(err)
	}

	// Make sure the last default takes precedence.
	var first *influxdb.DBRPMappingV2
	var lastDefault *influxdb.DBRPMappingV2
	if err := s.byOrgAndDatabase.Walk(ctx, tx, compKey, func(k, v []byte) error {
		curr := &influxdb.DBRPMappingV2{}
		if err := json.Unmarshal(v, curr); err != nil {
			return ErrInternalService(err)
		}
		if first == nil {
			first = curr
		}
		if curr.Default {
			if lastDefault != nil {
				lastDefault.Default = false
				bs, err := json.Marshal(lastDefault)
				if err != nil {
					return ErrInternalService(err)
				}
				id, _ := lastDefault.ID.Encode()
				if err := bucket.Put(id, bs); err != nil {
					return err
				}
			}
			lastDefault = curr
		}
		return nil
	}); err != nil {
		return err
	}

	// This means no default has been found.
	// Make the first one the default.
	// If that does not exists, then there is no DBRP.
	if lastDefault == nil && first != nil {
		first.Default = true
		bs, err := json.Marshal(first)
		if err != nil {
			return ErrInternalService(err)
		}
		id, _ := first.ID.Encode()
		return bucket.Put(id, bs)
	}
	return nil
}

// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
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
	if _, n, err := s.FindMany(ctx, influxdb.DBRPMappingFilterV2{
		OrgID:           &dbrp.OrganizationID,
		Database:        &dbrp.Database,
		RetentionPolicy: &dbrp.RetentionPolicy,
	}); err != nil {
		return err
	} else if n > 0 {
		return ErrDBRPAlreadyExists("another DBRP mapping with same orgID, db, and rp exists")
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
		return s.ensureOneDefault(ctx, tx, compKey)
	})
}

// Update a dbrp mapping.
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
		return s.ensureOneDefault(ctx, tx, indexForeignKey(*dbrp))
	})
}

// Delete removes a dbrp mapping.
// Deleting a mapping that does not exists is not an error.
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
		if err := bucket.Delete(encodedID); err != nil {
			return err
		}
		compKey := indexForeignKey(*dbrp)
		if err := s.byOrgAndDatabase.Delete(tx, compKey, encodedID); err != nil {
			return err
		}
		return s.ensureOneDefault(ctx, tx, compKey)
	})
}

// filterFunc is capable to validate if the dbrp is valid from a given filter.
// it runs true if the filtering data are contained in the dbrp
func filterFunc(dbrp *influxdb.DBRPMappingV2, filter influxdb.DBRPMappingFilterV2) bool {
	return (filter.ID == nil || (*filter.ID) == dbrp.ID) &&
		(filter.OrgID == nil || (*filter.OrgID) == dbrp.OrganizationID) &&
		(filter.BucketID == nil || (*filter.BucketID) == dbrp.BucketID) &&
		(filter.Database == nil || (*filter.Database) == dbrp.Database) &&
		(filter.RetentionPolicy == nil || (*filter.RetentionPolicy) == dbrp.RetentionPolicy) &&
		(filter.Default == nil || (*filter.Default) == dbrp.Default)
}
