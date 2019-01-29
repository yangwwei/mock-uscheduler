package main

import (
	"context"
	"fmt"
	"github.com/leftnoteasy/si-spec/lib/go/si"
	"io"
	"log"
	"time"

	pb "github.com/leftnoteasy/si-spec/lib/go/si"
	"google.golang.org/grpc"
)

const (
	address = "localhost:3333"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewSchedulerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour * 100000)
	defer cancel()
	_, err = c.RegisterResourceManager(ctx, &pb.RegisterResourceManagerRequest{})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Succeessfully registered")
	log.Printf("Start sending requests...")

	stream, err := c.Update(ctx)
	done := make(chan bool)

	// Connect to server and send streaming
	// first goroutine sends requests
	go func() {
		for i := 1; i <= 10; i++ {
			// req := si.UpdateRequest{}
			prefix := "20190129000"
			requestID := fmt.Sprintf("%s%d", prefix, i)
			req := populateSamples(requestID)
			if err := stream.Send(&req); err != nil {
				log.Fatalf("can not send %v", err)
			}

			log.Printf("Sending request, request: %s", req.String())
			time.Sleep(time.Millisecond * 1000)
		}
		//if err := stream.CloseSend(); err != nil {
		//   log.Println(err)
		//}
	}()

	// second goroutine receives data from stream
	// and saves result in max variable
	//
	// if stream is finished it closes done channel
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				close(done)
				return
			}
			if err != nil {
				log.Fatalf("can not receive %v", err)
			}
			log.Printf("Responded by server")
		}
	}()

	// third goroutine closes done channel
	// if context is done
	go func() {
		<-ctx.Done()
		if err := ctx.Err(); err != nil {
			log.Println(err)
		}
		close(done)
	}()

	<-done
	log.Printf("Finished")
}

// sample request
func populateSamples(AllocationKey string) si.UpdateRequest {
	resource := pb.Resource{
		Resources: map[string]*pb.Quantity{
			"memory" : { Value : 2048},
			"vcore" : {Value : 5},
		},
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	ask := pb.AllocationAsk{
		AllocationKey:                AllocationKey,
		ResourceAsk:                  &resource,
		Tags:                         nil,
		Priority:                     nil,
		ResourcePerAlloc:             nil,
		PlacementConstraint:          nil,
		MaxAllocations:               0,
		Ugi:                          nil,
		QueueName:                    "root",
		ExecutionTimeoutMilliSeconds: 0,
		XXX_NoUnkeyedLiteral:         struct{}{},
		XXX_unrecognized:             nil,
		XXX_sizecache:                0,
	}

	result := pb.UpdateRequest{
		Asks:                 []*pb.AllocationAsk {&ask},
		ReleaseAllocations:   nil,
		NewSchedulableNodes:  nil,
		UpdatedNodes:         nil,
		UtilizationReports:   nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	return result
}
