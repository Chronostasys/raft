package kvraft

import (
	"reflect"
	"testing"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/trees/query"
)

type Test struct {
	TestInt    int    `sql:"pk"`
	TestString string `idx:"-"`
	TestFloat  float32
}

func TestRow(t *testing.T) {
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := raft.MakeRPCEnds(ends)
	servers := make([]*KVServer, len(ends))
	for i := range rpcends {
		servers[i] = StartKVServer(rpcends, i, raft.MakePersister(), 10000)
		go servers[i].Serve(ends[i])
	}
	defer func() {
		for i := 0; i < len(ends); i++ {
			servers[i].Close()
		}
		time.Sleep(time.Millisecond * 100)
	}()
	c := MakeClerk(rpcends)
	InitQueryEngine(c)
	query.Register(&Test{})
	query.CreateTable(&Test{})
	q, _ := query.Table(&Test{})
	item := &Test{
		TestInt:    11,
		TestString: "test",
		TestFloat:  9.33,
	}
	item1 := &Test{
		TestInt:    10,
		TestString: "atest",
		TestFloat:  9.33,
	}
	q.Insert(item1)
	item2 := &Test{
		TestInt:    12,
		TestString: "test",
		TestFloat:  9.34,
	}
	q.Insert(item2)
	q.Insert(item)
	item3 := &Test{
		TestInt:    13,
		TestString: "btest",
		TestFloat:  9.34,
	}
	q.Insert(item3)
	t.Run("test find by pk", func(t *testing.T) {
		re := &Test{TestInt: 11}
		q.FindByPK(re)
		if !reflect.DeepEqual(re, item) {
			t.Errorf("expect search result %v, got %v", item, re)
		}
	})
	t.Run("test find by pk not found", func(t *testing.T) {
		re := &Test{TestInt: 9}
		err := q.FindByPK(re)
		if err.Error() != "not found" {
			t.Errorf("expect err not found, got %v", err)
		}
	})
	t.Run("test find multi", func(t *testing.T) {
		re := &Test{TestString: "test"}
		results := []*Test{}
		err := q.Find(re, 0, 10, func(i interface{}) bool {
			t := i.(*Test)
			results = append(results, &Test{
				TestInt:    t.TestInt,
				TestString: t.TestString,
				TestFloat:  t.TestFloat,
			})
			return true
		}, "TestString")
		items := []*Test{item, item2}
		if !reflect.DeepEqual(results, items) {
			t.Errorf("expect search result %v, got %v. err=%v", items, results, err)
		}
	})
	t.Run("test find multi skip", func(t *testing.T) {
		re := &Test{TestString: "test"}
		results := []*Test{}
		err := q.Find(re, 1, 10, func(i interface{}) bool {
			t := i.(*Test)
			results = append(results, &Test{
				TestInt:    t.TestInt,
				TestString: t.TestString,
				TestFloat:  t.TestFloat,
			})
			return true
		}, "TestString")
		items := []*Test{item2}
		if !reflect.DeepEqual(results, items) {
			t.Errorf("expect search result %v, got %v. err=%v", items, results, err)
		}
	})
	t.Run("test find multi limit", func(t *testing.T) {
		re := &Test{TestString: "test"}
		results := []*Test{}
		err := q.Find(re, 0, 1, func(i interface{}) bool {
			t := i.(*Test)
			results = append(results, &Test{
				TestInt:    t.TestInt,
				TestString: t.TestString,
				TestFloat:  t.TestFloat,
			})
			return true
		}, "TestString")
		items := []*Test{item}
		if !reflect.DeepEqual(results, items) {
			t.Errorf("expect search result %v, got %v. err=%v", items, results, err)
		}
	})
	t.Run("test find all", func(t *testing.T) {
		re := &Test{TestString: "test"}
		results := []*Test{}
		err := q.FindAll(re, &results, 0, 10, "TestString")
		items := []*Test{item, item2}
		if !reflect.DeepEqual(results, items) {
			t.Errorf("expect search result %v, got %v. err=%v", items, results, err)
		}
	})
	t.Run("test find by index", func(t *testing.T) {
		re := &Test{TestString: "test"}
		err := q.FindOne(re, "TestString")
		if !reflect.DeepEqual(re, item) {
			t.Errorf("expect search result %v, got %v. err=%v", item, re, err)
		}
	})
	t.Run("test find by multi query with single index", func(t *testing.T) {
		re := &Test{TestString: "test", TestFloat: 9.34}
		err := q.FindOne(re, "TestString", "TestFloat")
		if !reflect.DeepEqual(re, item2) {
			t.Errorf("expect search result %v, got %v. err=%v", item2, re, err)
		}
	})
	t.Run("test find no index", func(t *testing.T) {
		re := &Test{TestFloat: 9.34}
		err := q.FindOne(re, "TestFloat")
		if !reflect.DeepEqual(re, item2) {
			t.Errorf("expect search result %v, got %v. err=%v", item2, re, err)
		}
	})
	t.Run("test update", func(t *testing.T) {
		re := &Test{TestString: "btest"}
		i := &Test{
			TestInt:    11,
			TestString: "btest",
			TestFloat:  9.33,
		}
		q.Update(i, "TestString")
		err := q.FindOne(re, "TestString")
		if !reflect.DeepEqual(re, i) {
			t.Errorf("expect search result %v, got %v. err=%v", i, re, err)
		}
		re = &Test{TestString: "test"}
		err = q.FindOne(re, "TestString")
		if !reflect.DeepEqual(re, item2) {
			t.Errorf("expect search result %v, got %v. err=%v", item2, re, err)
		}
	})
}
