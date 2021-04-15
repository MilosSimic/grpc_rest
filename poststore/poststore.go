package poststore

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	helloworldpb "github.com/milossimic/grpc_rest/proto/helloworld"
	tracer "github.com/milossimic/grpc_rest/tracer"
	opentracing "github.com/opentracing/opentracing-go"
)

const (
	posts = "posts/%s"
	all   = "posts"
)

type PostStore struct {
	cli *api.Client
}

func New() (*PostStore, error) {
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, err
	}

	return &PostStore{
		cli: client,
	}, nil
}

func (ps *PostStore) Get(ctx context.Context, id string) (*helloworldpb.Post, error) {
	spanContext := tracer.ExtractSpanContextFromMetadata(opentracing.GlobalTracer(), ctx)
	span := opentracing.StartSpan(
		"Get",
		opentracing.ChildOf(spanContext),
	)
	defer span.Finish()

	kv := ps.cli.KV()
	pair, _, err := kv.Get(constructKey(ctx, id), nil)
	if err != nil {
		return nil, err
	}

	post := &helloworldpb.Post{}
	err = proto.Unmarshal(pair.Value, post)
	if err != nil {
		return nil, err
	}

	return post, nil
}

func (ps *PostStore) GetAll(ctx context.Context) (*helloworldpb.GetAllPosts, error) {
	spanContext := tracer.ExtractSpanContextFromMetadata(opentracing.GlobalTracer(), ctx)
	span := opentracing.StartSpan(
		"GetAll",
		opentracing.ChildOf(spanContext),
	)
	defer span.Finish()

	kv := ps.cli.KV()
	data, _, err := kv.List(all, nil)
	if err != nil {
		return nil, err
	}

	posts := []*helloworldpb.Post{}
	for _, pair := range data {
		post := &helloworldpb.Post{}
		err = proto.Unmarshal(pair.Value, post)
		if err != nil {
			return nil, err
		}
		posts = append(posts, post)
	}

	return &helloworldpb.GetAllPosts{
		Posts: posts,
	}, nil
}

func generateKey(ctx context.Context) (string, string) {
	span, _ := opentracing.StartSpanFromContext(ctx, "generateKey")
	defer span.Finish()

	id := uuid.New().String()
	return fmt.Sprintf(posts, id), id
}

func constructKey(ctx context.Context, id string) string {
	span, _ := opentracing.StartSpanFromContext(ctx, "constructKey")
	defer span.Finish()

	return fmt.Sprintf(posts, id)
}

func (ps PostStore) Post(ctx context.Context, post *helloworldpb.CreatePostRequest) (*helloworldpb.Post, error) {
	spanContext := tracer.ExtractSpanContextFromMetadata(opentracing.GlobalTracer(), ctx)
	span := opentracing.StartSpan(
		"Post",
		opentracing.ChildOf(spanContext),
	)
	defer span.Finish()

	kv := ps.cli.KV()
	sid, rid := generateKey(ctx)
	post.Post.Id = rid

	data, err := proto.Marshal(post.Post)
	if err != nil {
		return nil, err
	}

	ctx = opentracing.ContextWithSpan(context.Background(), span)
	p := &api.KVPair{Key: sid, Value: data}
	_, err = kv.Put(p, nil)
	if err != nil {
		return nil, err
	}

	return post.Post, nil
}

func (ps *PostStore) Delete(ctx context.Context, id string) (*helloworldpb.Post, error) {
	spanContext := tracer.ExtractSpanContextFromMetadata(opentracing.GlobalTracer(), ctx)
	span := opentracing.StartSpan(
		"Delete",
		opentracing.ChildOf(spanContext),
	)
	defer span.Finish()

	kv := ps.cli.KV()

	ctx = opentracing.ContextWithSpan(context.Background(), span)
	_, err := kv.Delete(constructKey(ctx, id), nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
