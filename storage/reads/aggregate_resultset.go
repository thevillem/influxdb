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

// TODO: remove those
type mockIntegerArrayCursor struct {
	eof bool
}

func (m *mockIntegerArrayCursor) Next() *cursors.IntegerArray {
	if m.eof {
		return &cursors.IntegerArray{}
	}
	m.eof = true
	return &cursors.IntegerArray{
		Timestamps: []int64{
			1, 2, 3, 4, 5, 6, 7,
		},
		Values: []int64{
			7, 6, 5, 4, 3, 2, 1,
		},
	}
}

func (*mockIntegerArrayCursor) Close() {

}
func (*mockIntegerArrayCursor) Err() error {
	return nil
}
func (*mockIntegerArrayCursor) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}

func (r *windowAggregateResultSet) Next() cursors.Cursor {
	//seriesRow := r.cursor.Next()
	//cursor := r.arrayCursors.createCursor(seriesRow)
	cursor := &mockIntegerArrayCursor{}
	return newWindowAggregateArrayCursor(r.ctx, r.req, cursor)
}

func (r *windowAggregateResultSet) Close() {
	// TODO: implement this
}

func (r *windowAggregateResultSet) Err() error { return nil }

// TODO: implement FloatWindowAggregateCountCursor
// TODO: implement IntegerWindowAggregateCountCursor
// TODO: implement FloatWindowAggregateCountCursor
