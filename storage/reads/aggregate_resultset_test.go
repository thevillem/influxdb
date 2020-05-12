package reads_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	phttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func _TestLauncher_WriteV2_Query(t *testing.T) {
	ctx := context.Background()
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// The default gateway instance inserts some values directly such that ID lookups seem to break,
	// so go the roundabout way to insert things correctly.
	req := l.MustNewHTTPRequest(
		"POST",
		fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID),
		fmt.Sprintf("ctr n=1i %d", time.Now().UnixNano()),
	)
	phttp.SetToken(l.Auth.Token, req)

	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	if resp.StatusCode != nethttp.StatusNoContent {
		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, resp.Body); err != nil {
			t.Fatalf("Could not read body: %s", err)
		}
		t.Fatalf("exp status %d; got %d, body: %s", nethttp.StatusNoContent, resp.StatusCode, buf.String())
	}

	res := l.MustExecuteQuery(fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, l.Bucket.Name))
	defer res.Done()
	res.HasTableCount(t, 1)
}

func _TestNewReadWindowAggregateResultSet_Count(t *testing.T) {
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
		Range: datatypes.TimestampRange{
			Start: 1,
			End:   8,
		},
	}

	rs, err := reads.NewWindowAggregateResultSet(context.Background(), &request, seriesCursor)
	for rs.Next() {
		cur := rs.Cursor()
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
