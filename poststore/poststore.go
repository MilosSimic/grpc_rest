package poststore

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	helloworldpb "github.com/milossimic/grpc_rest/proto/helloworld"
	"time"
)

const (
	posts = "/posts/%s"
	all   = "/posts"
)

type PostStore struct {
	cli *clientv3.Client
}

func New(*PostStore, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &PostStore{
		cli: cli,
	}, nil
}

func (ps *PostStore) Get(ctx context.Context, id string) (*helloworldpb.Post, error) {
	resp, err := ps.cli.Get(ctx, constructKey(id))
	cancel()
	if err != nil {
		return nil, err
	}

	for _, ev := range resp.Kvs {
		// fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}
}

func (ps *PostStore) GetAll(ctx context.Context) ([]*helloworldpb.Post, error) {
	resp, err := ps.cli.Get(ctx, all, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}
}

func generateKey(ctx context.Context) string {
	id := uuid.New().String()
	return fmt.Sprintf(posts, id)
}

func constructKey(ctx context.Context, id string) string {
	return fmt.Sprintf(posts, id)
}

func (ps *PostStore) Post(ctx context.Context, post *helloworldpb.Post) (*helloworldpb.Post, error) {
	data, err := proto.Marshal(post)
	if err != nil {
		return nil, err
	}

	_, err = cli.Put(ctx, generateKey(ctx), data)
	if err != nil {
		return nil, err
	}

	return post
}

func (ps *PostStore) Delete(ctx context.Contex, id string) (*helloworldpb.Post, error) {
	_, err := pscli.Delete(ctx, constructKey(id))
	if err != nil {
		return nil, err
	}
	return nil, nil
}
