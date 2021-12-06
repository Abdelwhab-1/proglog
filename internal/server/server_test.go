package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/abdelwhab-1/proglog/api/v1"
	"github.com/abdelwhab-1/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)

		})
	}
}

func setupTest(t *testing.T, fn func(conf *Config)) (api.LogClient, *Config, func()) {
	t.Helper()
	// have a listner to listen to free port
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	// create a grpc client that will listen to the same port as  L
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	client_con, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)
	// create new temp dir to use as a dir for a temp log from log package
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	// create a log.log
	cLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// create configuration to us to open a server
	cfg := &Config{
		CommitLog: cLog,
	}
	require.NoError(t, err)

	if fn != nil {
		fn(cfg)
	}
	// create new server to serve
	srv, err := NewGRPCServer(cfg)
	require.NoError(t, err)
	// start another go routin to serve incoming requrest to the listner we created above
	go func() {
		srv.Serve(l)
	}()
	// create a new client that can send request to the listner

	Client := api.NewLogClient(client_con)
	// return Client, it's confgurations, and a function to close everything and remove temp log
	return Client, cfg, func() {
		srv.Stop()
		client_con.Close()
		l.Close()
		cLog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{OffSet: produce.OffSet})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, produce.OffSet, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.OffSet != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.OffSet, offset)
			}

		}

	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{OffSet: 0})
		require.NoError(t, err)
		for _, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Value, res.Record.Value)
			require.Equal(t, record.Offset, uint64(res.Record.Offset))
		}

	}

}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {

	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{OffSet: produce.OffSet + 1})
	if consume != nil {
		t.Fatal("Consume is not nil: ")
	}
	got := grpc.Code(err)
	wanted := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != wanted {
		t.Fatalf("got err: %v, want: %v", got, wanted)
	}

}
