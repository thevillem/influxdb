package reads_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestNewReadWindowAggregateResultSet_Count(t *testing.T) {
	seriesCursor := &sliceSeriesCursor{
		rows: newSeriesRows(
			"aaa,tag0=val00",
			"aaa,tag0=val01",
		)}
	aggregates := make([]*datatypes.Aggregate, 1)
	aggregates[0] = &datatypes.Aggregate{Type: datatypes.AggregateTypeCount}
	request := datatypes.ReadWindowAggregateRequest{
		WindowEvery: 2,
		Aggregate:   aggregates,
	}

	rs, err := reads.NewWindowAggregateResultSet(context.Background(), &request, seriesCursor)
	for {
		cur := rs.Next()
		if cur == nil {
			continue
		}
		intcur := cur.(cursors.IntegerArrayCursor)
		// breakpoint here
		intcur.Next()
	}

	if rs == nil {
		t.Errorf("unexpected nil cursor")
	}
	if err != nil {
		t.Errorf("expected nil error")
	}
}

func _TestNewReadWindowAggregateResultSet_Sum(t *testing.T) {
	seriesCursor := &sliceSeriesCursor{
		rows: newSeriesRows(
			"aaa,tag0=val00",
			"aaa,tag0=val01",
		)}
	aggregates := make([]*datatypes.Aggregate, 1)
	aggregates[0] = &datatypes.Aggregate{Type: datatypes.AggregateTypeSum}
	request := datatypes.ReadWindowAggregateRequest{
		WindowEvery: 2,
		Aggregate:   aggregates,
	}

	results, err := reads.NewWindowAggregateResultSet(context.Background(), &request, seriesCursor)

	if results == nil {
		t.Errorf("unexpected nil cursor")
	}
	if err != nil {
		t.Errorf("expected nil error")
	}
}
