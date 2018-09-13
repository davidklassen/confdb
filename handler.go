package main

import (
	"bytes"
	"github.com/davidklassen/confdb/storage"
	"io/ioutil"
	"net/http"
)

type Handler struct {
	storage *storage.Storage
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r.URL.Path)
	case http.MethodPut:
		if data, err := ioutil.ReadAll(r.Body); err != nil {
			h.write(w, http.StatusInternalServerError, nil)
		} else {
			h.handlePut(w, r.URL.Path, data)
		}
	case http.MethodDelete:
		h.handleDelete(w, r.URL.Path)
	default:
		code := http.StatusMethodNotAllowed
		w.WriteHeader(code)
		w.Write([]byte(http.StatusText(code)))
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, path string) {
	if path[len(path)-1] == '/' {
		if l, err := h.storage.List(path[:len(path)-1]); err == storage.ErrNotFound {
			h.write(w, http.StatusNotFound, nil)
		} else if err != nil {
			h.write(w, http.StatusInternalServerError, nil)
		} else {
			var buf bytes.Buffer
			for _, path := range l {
				buf.Write([]byte(path))
				buf.Write([]byte("\n"))
			}
			h.write(w, http.StatusOK, buf.Bytes())
		}
	} else {
		if data, err := h.storage.Get(path); err == storage.ErrNotFound {
			h.write(w, http.StatusNotFound, nil)
		} else if err != nil {
			h.write(w, http.StatusInternalServerError, nil)
		} else {
			h.write(w, http.StatusOK, data)
		}
	}
}

func (h *Handler) handlePut(w http.ResponseWriter, path string, data []byte) {
	code := http.StatusNoContent
	if err := h.storage.Put(path, data); err != nil {
		code = http.StatusInternalServerError
	}
	h.write(w, code, nil)
}

func (h *Handler) handleDelete(w http.ResponseWriter, path string) {
	if err := h.storage.Delete(path); err != nil {
		h.write(w, http.StatusInternalServerError, nil)
	}
}

func (h *Handler) write(w http.ResponseWriter, code int, data []byte) {
	w.WriteHeader(code)
	if data != nil {
		w.Write(data)
	} else if code != http.StatusNoContent {
		w.Write([]byte(http.StatusText(code)))
	}
}
