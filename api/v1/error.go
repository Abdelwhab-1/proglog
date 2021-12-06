package api_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	OffSet uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("index %d out of range ", e.OffSet))
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", e.OffSet)
	d := &errdetails.LocalizedMessage{
		Message: msg,
		Locale:  "en-US",
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
