package kvraft

import "github.com/Chronostasys/trees/query"

type kvClient struct {
	*Clerk
}

func (c *kvClient) Insert(k, v string) {
	c.Put(k, v)
}

func (c *kvClient) Delete(k string) {
	c.PutAppend(k, "", "Delete")
}
func (c *kvClient) Search(k string) string {
	return c.Get(k)
}

func InitQueryEngine(c *Clerk) {
	query.SetKV(&kvClient{
		Clerk: c,
	})
}
