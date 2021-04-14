package main

import (
	"context"
	"log"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"

	ps "github.com/milossimic/grpc_rest/poststore"
	helloworldpb "github.com/milossimic/grpc_rest/proto/helloworld"
)

type server struct {
	helloworldpb.UnimplementedGreeterServer
	store *ps.PostStore
}

func NewServer() (*server, error) {
	store, err := ps.New()
	if err != nil {
		return nil, err
	}

	return &server{
		store: store,
	}, nil
}

func (s *server) PostRequest(ctx context.Context, in *helloworldpb.CreatePostRequest) (*helloworldpb.Post, error) {
	return s.store.Post(ctx, in)
}

func (s *server) GetRequest(ctx context.Context, in *helloworldpb.GetPostRequest) (*helloworldpb.Post, error) {
	return s.store.Get(ctx, in.Post)
}

func (s *server) GetAllRequest(ctx context.Context, in *helloworldpb.EmptyRequest) (*helloworldpb.GetAllPosts, error) {
	return s.store.GetAll(ctx)
}

func (s *server) DeleteRequest(ctx context.Context, in *helloworldpb.DeletePostRequest) (*helloworldpb.Post, error) {
	return s.store.Delete(ctx, in.Post)
}

func main() {
	// Create a listener on TCP port
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}

	// Create a gRPC server object
	s := grpc.NewServer()

	service, err := NewServer()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// Attach the Greeter service to the server
	helloworldpb.RegisterGreeterServer(s, service)
	// Serve gRPC server
	log.Println("Serving gRPC on 0.0.0.0:8080")
	go func() {
		log.Fatalln(s.Serve(lis))
	}()

	// Create a client connection to the gRPC server we just started
	// This is where the gRPC-Gateway proxies the requests
	conn, err := grpc.DialContext(
		context.Background(),
		"0.0.0.0:8080",
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalln("Failed to dial server:", err)
	}

	gwmux := runtime.NewServeMux()
	// Register Greeter
	err = helloworldpb.RegisterGreeterHandler(context.Background(), gwmux, conn)
	if err != nil {
		log.Fatalln("Failed to register gateway:", err)
	}

	gwServer := &http.Server{
		Addr:    ":8090",
		Handler: gwmux,
	}

	log.Println("Serving gRPC-Gateway on http://0.0.0.0:8090")
	log.Fatalln(gwServer.ListenAndServe())
}
