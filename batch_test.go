package bolt_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func withDB(t testing.TB, fn func(*bolt.DB)) {
	tmp, err := ioutil.TempFile("", "bolt-batch-test-")
	if err != nil {
		t.Fatal(err)
	}
	_ = tmp.Close()
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	db, err := bolt.Open(tmp.Name(), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	fn(db)
}

func withBucket(t testing.TB, db *bolt.DB, name string, fn func(*bolt.DB)) {
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket([]byte(name))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fn(db)
}

func TestSimple(t *testing.T) {
	withDB(t, func(db *bolt.DB) {
		withBucket(t, db, "widgets", func(db *bolt.DB) {
			errCh := make(chan error)
			one := func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("widgets")).Put([]byte("one"), []byte("ONE"))
			}
			go func() {
				errCh <- db.Batch(one)
			}()
			two := func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("widgets")).Put([]byte("two"), []byte("TWO"))
			}
			go func() {
				errCh <- db.Batch(two)
			}()

			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}

			if err := db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("widgets"))
				if g, e := string(bucket.Get([]byte("one"))), "ONE"; g != e {
					t.Errorf("bad content: %q != %q", g, e)
				}
				if g, e := string(bucket.Get([]byte("two"))), "TWO"; g != e {
					t.Errorf("bad content: %q != %q", g, e)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func TestPanic(t *testing.T) {
	withDB(t, func(db *bolt.DB) {
		var sentinel int
		var bork = &sentinel
		var problem interface{}
		var err error
		func() {
			defer func() {
				if p := recover(); p != nil {
					problem = p
				}
			}()
			err = db.Batch(func(tx *bolt.Tx) error {
				panic(bork)
			})
		}()
		if g, e := err, error(nil); g != e {
			t.Fatalf("wrong error: %v != %v", g, e)
		}
		if g, e := problem, bork; g != e {
			t.Fatalf("wrong error: %v != %v", g, e)
		}
	})
}
