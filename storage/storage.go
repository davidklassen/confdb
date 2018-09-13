package storage

import (
	"errors"
	"strings"
)

var ErrNotFound = errors.New("object not found")

var listSeparator = "/"

type kind int

const (
	dir kind = iota
	obj
)

type object struct {
	kind     kind
	name     string
	value    []byte
	children map[string]*object
	parent   *object
}

func (o *object) getPath() string {
	if o.parent == nil {
		return ""
	}
	return o.parent.getPath() + "/" + o.name
}

func (o *object) find(path string) (*object, error) {
	cur := o
	parts := strings.Split(path, listSeparator)[1:]
	for _, part := range parts {
		var ok bool
		if cur, ok = cur.children[part]; !ok {
			return nil, ErrNotFound
		}
	}
	return cur, nil
}

func (o *object) findClosest(path string) (*object, []string) {
	cur := o
	parts := strings.Split(path, listSeparator)[1:]
	for i, part := range parts {
		next, ok := cur.children[part]
		if !ok {
			return cur, parts[i:]
		}
		cur = next
	}
	return cur, []string{}
}

type Storage struct {
	root *object
}

func New() *Storage {
	return &Storage{root: &object{
		children: map[string]*object{},
		parent:   nil,
	}}
}

func (s *Storage) List(path string) ([]string, error) {
	if path == "/" {
		path = ""
	}
	o, err := s.root.find(path)
	if err != nil {
		return nil, err
	}
	if o.kind != dir {
		return nil, ErrNotFound
	}
	names := make([]string, len(o.children))
	i := 0
	for _, child := range o.children {
		names[i] = child.getPath()
		i++
	}
	return names, nil
}

func (s *Storage) Put(path string, data []byte) error {
	o, tail := s.root.findClosest(path)
	if len(tail) != 0 && o.kind == obj {
		o.value = nil
		o.kind = dir
		o.children = make(map[string]*object)
	}
	for _, name := range tail {
		o.children[name] = &object{
			kind:     dir,
			name:     name,
			children: make(map[string]*object),
			parent:   o,
		}
		o = o.children[name]
	}
	o.children = nil
	o.kind = obj
	o.value = data
	return nil
}

func (s *Storage) Get(path string) ([]byte, error) {
	o, err := s.root.find(path)
	if err != nil {
		return nil, err
	}
	if o.kind != obj {
		return nil, ErrNotFound
	}
	return o.value, nil
}

func (s *Storage) Delete(path string) error {
	o, err := s.root.find(path)
	if err != nil {
		return err
	}
	delete(o.parent.children, o.name)
	return nil
}
