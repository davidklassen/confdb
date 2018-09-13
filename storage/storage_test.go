package storage_test

import (
	"github.com/davidklassen/confdb/storage"
	"testing"
)

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("unexpected error %+v", err)
	}
}

func checkContainsString(t *testing.T, list []string, str string) {
	for _, el := range list {
		if el == str {
			return
		}
	}
	t.Errorf("expected %+v to contain %s", list, str)
}

func TestStorage(t *testing.T) {
	s := storage.New()
	testData := []byte("test data")
	testPath := "/foo"

	t.Run("list empty storage", func(t *testing.T) {
		l, err := s.List("/")
		checkErr(t, err)
		if l == nil {
			t.Error("expected empty list, got nil")
		}
		if len(l) != 0 {
			t.Errorf("expected empty list, got %+v", l)
		}
	})

	t.Run("list nonexistent path", func(t *testing.T) {
		l, err := s.List(testPath)
		if l != nil {
			t.Errorf("expected nil list, got %+v", l)
		}
		if err != storage.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %+v", err)
		}
	})

	t.Run("get nonexistent object", func(t *testing.T) {
		data, err := s.Get(testPath)
		if data != nil {
			t.Errorf("expected nil data, got %+v", data)
		}
		if err != storage.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %+v", err)
		}
	})

	t.Run("delete nonexistent object", func(t *testing.T) {
		err := s.Delete(testPath)
		if err != storage.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %+v", err)
		}
	})

	t.Run("put object", func(t *testing.T) {
		err := s.Put(testPath, testData)
		checkErr(t, err)
	})

	t.Run("get object", func(t *testing.T) {
		data, err := s.Get(testPath)
		checkErr(t, err)
		if string(data) != string(testData) {
			t.Errorf("expected %s, got %s", testData, data)
		}
	})

	t.Run("list storage", func(t *testing.T) {
		l, err := s.List("/")
		checkErr(t, err)
		if len(l) != 1 {
			t.Errorf("expected one element, got %d", len(l))
		}
		if l[0] != testPath {
			t.Errorf("expected %s, got %s", testPath, l[0])
		}
	})

	t.Run("delete object", func(t *testing.T) {
		err := s.Delete(testPath)
		checkErr(t, err)
	})

	t.Run("get after delete", func(t *testing.T) {
		data, err := s.Get(testPath)
		if data != nil {
			t.Errorf("expected nil data, got %+v", data)
		}
		if err != storage.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %+v", err)
		}
	})

	t.Run("list after delete", func(t *testing.T) {
		l, err := s.List("/")
		checkErr(t, err)
		if l == nil {
			t.Error("expected empty list, got nil")
		}
		if len(l) != 0 {
			t.Errorf("expected empty list, got %+v", l)
		}
	})
}

func TestMultipleObjects(t *testing.T) {
	s := storage.New()
	testData := []byte("test data")
	path1 := "/foo"
	path2 := "/bar"
	checkErr(t, s.Put(path1, testData))
	checkErr(t, s.Put(path2, testData))
	l, err := s.List("/")
	checkErr(t, err)
	if len(l) != 2 {
		t.Errorf("expected %+v to contain 2 elements", l)
	}
	checkContainsString(t, l, path1)
	checkContainsString(t, l, path2)
}

func TestNestedObjects(t *testing.T) {
	s := storage.New()
	testData := []byte("test data")
	path1 := "/foo/bar1"
	path2 := "/foo/bar2"
	checkErr(t, s.Put(path1, testData))
	checkErr(t, s.Put(path2, testData))
	l, err := s.List("/")
	checkErr(t, err)
	checkContainsString(t, l, "/foo")
	l, err = s.List("/foo")
	checkErr(t, err)
	checkContainsString(t, l, "/foo/bar1")
	checkContainsString(t, l, "/foo/bar2")
}

func TestOverwritingNestedObject(t *testing.T) {
	s := storage.New()
	testData := []byte("test data")
	path1 := "/foo"
	path2 := "/foo/bar"
	checkErr(t, s.Put(path1, testData))
	checkErr(t, s.Put(path2, testData))
	l, err := s.List("/")
	checkErr(t, err)
	checkContainsString(t, l, path1)
	l, err = s.List(path1)
	checkErr(t, err)
	checkContainsString(t, l, path2)
}
