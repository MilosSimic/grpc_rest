package poststore

import (
	"github.com/coreos/etcd/clientv3"
)

type PostStore struct {
}

func New(*PostStore, error) {
	return &PostStore{}, nil
}
