package reads

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type windowAggregateResultSet struct {
	ctx          context.Context
	req          *datatypes.ReadWindowAggregateRequest
	i            int
	arrayCursors *arrayCursors
}

func NewWindowAggregateResultSet(ctx context.Context, req *datatypes.ReadWindowAggregateRequest, cursor SeriesCursor) (ReadWindowAggregateResultSet, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	for _, aggregate := range req.Aggregate {
		span.LogKV("aggregate_type", aggregate.String())
	}

	results := &windowAggregateResultSet{
		ctx:          ctx,
		req:          req,
		i:            0,
		arrayCursors: newArrayCursors(ctx, req.Range.Start, req.Range.End, true),
	}
	return results, nil
}

func (r *windowAggregateResultSet) Next() cursors.Cursor {
	// TODO: implement this entirely
	//return newAggregateArrayCursor(ctx, aggregate, cursor)
	return nil
}

func (r *windowAggregateResultSet) Close() {
	// TODO: implement this
}

func (r *windowAggregateResultSet) Err() error { return nil }
