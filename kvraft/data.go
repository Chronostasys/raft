package kvraft

import "hash/fnv"

type Doc struct {
	Key int
	Val string
}

func (d *Doc) Hash() int {
	return d.Key
}

func makeDoc(key, val string) *Doc {
	return &Doc{
		Key: hash(key),
		Val: val,
	}
}
func hash(s string) int {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int(h.Sum64())
}
