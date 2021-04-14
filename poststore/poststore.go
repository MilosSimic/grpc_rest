package poststore

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	helloworldpb "github.com/milossimic/grpc_rest/proto/helloworld"
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
	post.Id = pair.Key

	return post, nil
}

func (ps *PostStore) GetAll(ctx context.Context) (*helloworldpb.GetAllPosts, error) {
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
		post.Id = pair.Key
		posts = append(posts, post)
	}

	return &helloworldpb.GetAllPosts{
		Posts: posts,
	}, nil
}

func generateKey(ctx context.Context) string {
	id := uuid.New().String()
	return fmt.Sprintf(posts, id)
}

func constructKey(ctx context.Context, id string) string {
	return fmt.Sprintf(posts, id)
}

func (ps PostStore) Post(ctx context.Context, post *helloworldpb.CreatePostRequest) (*helloworldpb.Post, error) {
	kv := ps.cli.KV()

	data, err := proto.Marshal(post)
	if err != nil {
		return nil, err
	}

	p := &api.KVPair{Key: generateKey(ctx), Value: data}
	_, err = kv.Put(p, nil)
	if err != nil {
		return nil, err
	}

	return post.Post, nil
}

func (ps *PostStore) Delete(ctx context.Context, id string) (*helloworldpb.Post, error) {
	kv := ps.cli.KV()
	_, err := kv.Delete(constructKey(ctx, id), nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
