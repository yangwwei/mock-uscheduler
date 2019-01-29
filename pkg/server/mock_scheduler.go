package main

import (
	"github.com/leftnoteasy/si-spec/lib/go/si"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
)

type MockScheduler struct {
	SchedulerName string
}

func newSchedulerServer() si.SchedulerServer {
	return &MockScheduler{}
}

func (scheduler *MockScheduler) RegisterResourceManager(ctx context.Context,
	in *si.RegisterResourceManagerRequest) (*si.RegisterResourceManagerResponse, error) {
	log.Printf("Received registeration")
	return &(si.RegisterResourceManagerResponse{}), nil
}

func (scheduler *MockScheduler) Update(conn si.Scheduler_UpdateServer) error {
	log.Printf("start new server %s", scheduler.SchedulerName)
	ctx := conn.Context()

	for {

		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// receive data from stream
		message, err := conn.Recv()

		log.Printf("Requested recived, numberOfRequest : %d, request: %s",
			len(message.GetAsks()), message.String())

		if err == io.EOF {
			// return will close stream from server side
			log.Println("exit")
			return nil
		}
		if err != nil {
			log.Printf("receive error %v", err)
			continue
		}

		// Send response to stream
		resp := si.UpdateResponse{}

		log.Println(resp.String())

		//time.Sleep(2 * time.Second)

		if err := conn.Send(&resp); err != nil {
			log.Printf("send error %v", err)
			return err
		}

		log.Printf("Responded")
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:3333")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	si.RegisterSchedulerServer(s, &MockScheduler{"dummy-unified-scheduler"})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}