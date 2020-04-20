package dbrp

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	bucket = []byte("dbrpv1")
)

var _ influxdb.DBRPMappingServiceV2 = (*AuthorizedService)(nil)

type Service struct {
	store     kv.Store
	bucketSvc influxdb.BucketService
	IDGen     influxdb.IDGenerator
}

func NewService(ctx context.Context, bucketSvc influxdb.BucketService, st kv.Store) (influxdb.DBRPMappingServiceV2, error) {
	if err := st.Update(ctx, func(tx kv.Tx) error {
		_, err := tx.Bucket(bucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &Service{
		store:     st,
		bucketSvc: bucketSvc,
		IDGen:     snowflake.NewDefaultIDGenerator(),
	}, nil
}

// FindBy returns the dbrp mapping the for cluster, db and rp.
func (s *Service) FindByID(ctx context.Context, orgID, id influxdb.ID) (*influxdb.DBRPMappingV2, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidDBRPID
	}

	b := []byte{}

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
	err := s.store.View(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		cur, err := bucket.Cursor()
		if err != nil {
			return ErrInternalService(err)
		}

		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			dbrp := &influxdb.DBRPMappingV2{}
			if err := json.Unmarshal(v, dbrp); err != nil {
				return ErrInternalService(err)
			}
			if filterFunc(dbrp, filter) {
				dbrps = append(dbrps, dbrp)
			}
		}
		return nil
	})
	if err != nil {
		return nil, len(dbrps), err
	}
	return dbrps, len(dbrps), nil
}

// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
// If the mapping already contains a valid ID that is used for storing the mapping.
func (s *Service) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if !dbrp.ID.Valid() {
		dbrp.ID = s.IDGen.ID()
	}
	if err := dbrp.Validate(); err != nil {
		return ErrInvalidDBRP(err)
	}
	encodedID, err := dbrp.ID.Encode()
	if err != nil {
		return ErrInvalidDBRPID
	}
	b, err := json.Marshal(dbrp)
	if err != nil {
		return ErrInternalService(err)
	}

	if _, err := s.bucketSvc.FindBucketByID(ctx, dbrp.BucketID); err != nil {
		return err
	}

	// if a dbrp with this particular ID already exists an error is returned
	if _, err := s.FindByID(ctx, dbrp.OrganizationID, dbrp.ID); err == nil {
		return ErrDBRPAlreadyExists(err)
	}
	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		return bucket.Put(encodedID, b)
	})
}

// Update a dbrp mapping
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
		return bucket.Put(encodedID, b)
	})
}

// Delete removes a dbrp mapping.
// Deleting a mapping that does not exists is not an error.
func (s *Service) Delete(ctx context.Context, orgID, id influxdb.ID) error {
	if _, err := s.FindByID(ctx, orgID, id); err != nil {
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
		return bucket.Delete(encodedID)
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
