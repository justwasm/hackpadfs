//go:build js

package opfs

import (
	"encoding/json"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"github.com/hack-pad/hackpadfs"
)

const metaFileName = "#meta"

type fileMetadata struct {
	Mode  hackpadfs.FileMode `json:"mode"`
	Mtime time.Time          `json:"mtime"`
	Atime time.Time          `json:"atime"`
	Uid   int                `json:"uid,omitempty"`
	Gid   int                `json:"gid,omitempty"`
}

// metadataStore persists Unix file metadata in a hidden #meta file within OPFS.
// Uses JSON format for simplicity (no extra dependencies).
// Writes are debounced (100ms) to avoid excessive OPFS write operations.
type metadataStore struct {
	mu      sync.RWMutex
	data    map[string]fileMetadata
	root    js.Value
	writeCh chan struct{}
}

func newMetadataStore() *metadataStore {
	ms := &metadataStore{
		data:    make(map[string]fileMetadata),
		writeCh: make(chan struct{}, 1),
	}
	go ms.writeLoop()
	return ms
}

func (m *metadataStore) load(root js.Value) error {
	m.mu.Lock()
	m.root = root
	m.mu.Unlock()

	fileHandle, err := awaitErr(root.Call("getFileHandle", metaFileName, map[string]any{"create": false}))
	if err != nil {
		if isOPFSNotFound(err) {
			return nil // No existing metadata
		}
		return err
	}

	file, err := awaitErr(fileHandle.Call("getFile"))
	if err != nil {
		return err
	}

	buf, err := awaitErr(file.Call("arrayBuffer"))
	if err != nil {
		return err
	}

	jsBuf := js.Global().Get("Uint8Array").New(buf)
	data := make([]byte, jsBuf.Length())
	js.CopyBytesToGo(data, jsBuf)

	var metadata map[string]fileMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil // Reset on corruption
	}

	m.mu.Lock()
	m.data = metadata
	m.mu.Unlock()
	return nil
}

func (m *metadataStore) writeLoop() {
	for range m.writeCh {
		// Debounce: accumulate writes within 100ms
		time.Sleep(100 * time.Millisecond)

		// Drain any accumulated signals (non-blocking)
		for len(m.writeCh) > 0 {
			<-m.writeCh
		}

		// Wait until root is set (defensive guard)
		m.mu.RLock()
		data := make(map[string]fileMetadata, len(m.data))
		for k, v := range m.data {
			data[k] = v
		}
		root := m.root
		m.mu.RUnlock()

		if root.Type() == js.TypeUndefined {
			continue
		}

		b, err := json.Marshal(data)
		if err != nil {
			continue
		}

		fileHandle, err := awaitErr(root.Call("getFileHandle", metaFileName, map[string]any{"create": true}))
		if err != nil {
			continue
		}

		writable, err := awaitErr(fileHandle.Call("createWritable", map[string]any{"keepExistingData": false}))
		if err != nil {
			continue
		}

		jsBuf := js.Global().Get("Uint8Array").New(len(b))
		js.CopyBytesToJS(jsBuf, b)
		_, err = awaitErr(writable.Call("write", map[string]any{"type": "write", "data": jsBuf, "position": 0}))
		if err != nil {
			writable.Call("close")
			continue
		}
		awaitErr(writable.Call("close"))
	}
}

func (m *metadataStore) get(path string) (fileMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.data[path]
	return meta, ok
}

func (m *metadataStore) set(path string, meta fileMetadata) {
	m.mu.Lock()
	m.data[path] = meta
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) setMode(path string, mode hackpadfs.FileMode) {
	m.mu.Lock()
	meta, ok := m.data[path]
	if !ok {
		meta = fileMetadata{Mode: mode, Mtime: time.Now(), Atime: time.Now()}
	} else {
		meta.Mode = mode
	}
	m.data[path] = meta
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) setTimes(path string, atime, mtime time.Time) {
	m.mu.Lock()
	meta, ok := m.data[path]
	if !ok {
		meta = fileMetadata{Mode: 0644, Mtime: mtime, Atime: atime}
	} else {
		meta.Mtime = mtime
		meta.Atime = atime
	}
	m.data[path] = meta
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) setChown(path string, uid, gid int) {
	m.mu.Lock()
	meta, ok := m.data[path]
	if !ok {
		meta = fileMetadata{Mode: 0644, Mtime: time.Now(), Atime: time.Now(), Uid: uid, Gid: gid}
	} else {
		meta.Uid = uid
		meta.Gid = gid
	}
	m.data[path] = meta
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) del(path string) {
	m.mu.Lock()
	delete(m.data, path)
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) delPrefix(prefix string) {
	m.mu.Lock()
	prefix += "/"
	for k := range m.data {
		if strings.HasPrefix(k, prefix) || k == prefix[:len(prefix)-1] {
			delete(m.data, k)
		}
	}
	m.mu.Unlock()
	m.scheduleWrite()
}

func (m *metadataStore) scheduleWrite() {
	select {
	case m.writeCh <- struct{}{}:
	default:
	}
}

// reset clears all metadata in-place (avoids leaking the writeLoop goroutine).
func (m *metadataStore) reset(root js.Value) {
	m.mu.Lock()
	m.data = make(map[string]fileMetadata)
	m.root = root
	m.mu.Unlock()
}

// awaitErr awaits a JS promise and returns the result or error.
func awaitErr(promise js.Value) (js.Value, error) {
	ch := make(chan struct {
		result js.Value
		err    error
	}, 1)

	resolveFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		ch <- struct {
			result js.Value
			err    error
		}{result: args[0]}
		return nil
	})
	rejectFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		ch <- struct {
			result js.Value
			err    error
		}{err: js.Error{Value: args[0]}}
		return nil
	})

	promise.Call("then", resolveFn, rejectFn)
	r := <-ch
	resolveFn.Release()
	rejectFn.Release()
	return r.result, r.err
}

func isOPFSNotFound(err error) bool {
	if jsErr, ok := err.(js.Error); ok {
		return jsErr.Value.Get("name").String() == "NotFoundError"
	}
	return false
}
