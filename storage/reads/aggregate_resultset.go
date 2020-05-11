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
	cursor       SeriesCursor
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
		cursor:       cursor,
		i:            0,
		arrayCursors: newArrayCursors(ctx, req.Range.Start, req.Range.End, true),
	}
	return results, nil
}

func (r *windowAggregateResultSet) Next() cursors.Cursor {
	seriesRow := r.cursor.Next()
	//cursor := r.arrayCursors.createCursor(seriesRow)
	cursor := integerArrayCursor{}
	return newAggregateArrayCursor(ctx, aggregate, cursor)
}

func (r *windowAggregateResultSet) Close() {
	// TODO: implement this
}

func (r *windowAggregateResultSet) Err() error { return nil }

// TODO: implement FloatWindowAggregateCountCursor
// TODO: implement IntegerWindowAggregateCountCursor
// TODO: implement FloatWindowAggregateCountCursor
