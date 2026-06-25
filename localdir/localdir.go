//go:build js

// Package localdir implements hackpadfs.FS backed by the browser's File System Access API,
// allowing users to mount real local directories (picked via showDirectoryPicker()) into the
// virtual filesystem for access from Go WASM programs.
package localdir

import (
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"github.com/hack-pad/hackpadfs"
)

// FS implements hackpadfs.FS backed by a FileSystemDirectoryHandle from the
// File System Access API. It provides read/write access to a user-picked local directory.
//
// Supported interfaces:
//   - hackpadfs.FS (Open)
//   - hackpadfs.OpenFileFS (OpenFile)
//   - hackpadfs.MkdirFS (Mkdir)
//   - hackpadfs.MkdirAllFS (MkdirAll)
//   - hackpadfs.RemoveFS (Remove)
//   - hackpadfs.RemoveAllFS (RemoveAll)
//   - hackpadfs.RenameFS (Rename)
//   - hackpadfs.StatFS (Stat)
//   - hackpadfs.ChmodFS (Chmod)
//   - hackpadfs.ChtimesFS (Chtimes)
type FS struct {
	root   js.Value // FileSystemDirectoryHandle
	mode   string   // "read" or "readwrite"
	meta   *metadataStore
	cache  *statCache
	mu     sync.Mutex
}

type statCacheEntry struct {
	info     hackpadfs.FileInfo
	err      error
	cachedAt time.Time
}

type statCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]*statCacheEntry
}

func newStatCache() *statCache {
	return &statCache{
		ttl:     500 * time.Millisecond,
		entries: make(map[string]*statCacheEntry),
	}
}

func (c *statCache) get(key string) (hackpadfs.FileInfo, error, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Since(e.cachedAt) > c.ttl {
		return nil, nil, false
	}
	return e.info, e.err, true
}

func (c *statCache) set(key string, info hackpadfs.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = &statCacheEntry{info: info, cachedAt: time.Now()}
}

func (c *statCache) setErr(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = &statCacheEntry{err: err, cachedAt: time.Now()}
}

func (c *statCache) invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

func (c *statCache) invalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix += "/"
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) || k == prefix[:len(prefix)-1] {
			delete(c.entries, k)
		}
	}
}

type fileMetadata struct {
	Mode  hackpadfs.FileMode `json:"mode"`
	Mtime time.Time          `json:"mtime"`
	Atime time.Time          `json:"atime"`
}

// metadataStore stores file metadata for the localdir filesystem.
// Uses a simple in-memory map (not persisted to disk).
type metadataStore struct {
	mu   sync.RWMutex
	data map[string]fileMetadata
}

func newMetadataStore() *metadataStore {
	return &metadataStore{
		data: make(map[string]fileMetadata),
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
}

func (m *metadataStore) del(path string) {
	m.mu.Lock()
	delete(m.data, path)
	m.mu.Unlock()
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
}

// NewFS creates a localdir filesystem from a FileSystemDirectoryHandle.
// The mode parameter specifies the permission mode: "read" or "readwrite".
// The handle must have been obtained via showDirectoryPicker() or similar.
func NewFS(root js.Value, mode string) (*FS, error) {
	if root.Type() != js.TypeObject {
		return nil, errors.New("localdir: root must be a FileSystemDirectoryHandle")
	}
	return &FS{
		root:  root,
		mode:  mode,
		meta:  newMetadataStore(),
		cache: newStatCache(),
	}, nil
}

// ensureWritePermission checks and requests write permission if needed.
func (f *FS) ensureWritePermission() error {
	if f.mode != "readwrite" {
		return hackpadfs.ErrPermission
	}
	result, err := awaitErr(f.root.Call("requestPermission", map[string]any{"mode": "readwrite"}))
	if err != nil {
		return err
	}
	if result.String() != "granted" {
		return hackpadfs.ErrPermission
	}
	return nil
}

// Open implements hackpadfs.FS.
func (f *FS) Open(name string) (hackpadfs.File, error) {
	return f.openFile(name, false)
}

// OpenFile implements hackpadfs.OpenFileFS.
func (f *FS) OpenFile(name string, flag int, perm hackpadfs.FileMode) (hackpadfs.File, error) {
	create := flag&hackpadfs.FlagCreate != 0 || flag&hackpadfs.FlagAppend != 0
	readonly := flag&hackpadfs.FlagWriteOnly == 0 && flag&hackpadfs.FlagReadWrite == 0
	truncate := flag&hackpadfs.FlagTruncate != 0
	appendMode := flag&hackpadfs.FlagAppend != 0

	if create || !readonly {
		if err := f.ensureWritePermission(); err != nil {
			return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
		}
	}

	if name == "." {
		return &localdirFile{
			fsys:  f,
			name:  ".",
			handle: f.root,
			isDir: true,
		}, nil
	}

	dirPath := path.Dir(name)
	baseName := path.Base(name)

	dirHandle, err := f.walkDir(dirPath)
	if err != nil {
		if create {
			if err := f.MkdirAll(dirPath, 0755); err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
			dirHandle, err = f.walkDir(dirPath)
			if err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
		} else {
			return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: hackpadfs.ErrNotExist}
		}
	}

	if create {
		if flag&hackpadfs.FlagExclusive != 0 {
			if _, err := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": false})); err == nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: hackpadfs.ErrExist}
			} else if !isNotFound(err) {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
		}

		fileHandle, err := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": true}))
		if err != nil {
			return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
		}

		if truncate && !readonly {
			writable, err := awaitErr(fileHandle.Call("createWritable", map[string]any{"keepExistingData": false}))
			if err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
			if _, err := awaitErr(writable.Call("close")); err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
		}

		f.cache.invalidate(name)
		f.meta.setMode(name, perm)

		var writeOffset int64
		if appendMode {
			file, err := awaitErr(fileHandle.Call("getFile"))
			if err == nil {
				writeOffset = int64(file.Get("size").Float())
			}
		}

		return &localdirFile{
			fsys:     f,
			name:     name,
			handle:   fileHandle,
			append:   appendMode,
			readonly: readonly,
			offset:   writeOffset,
		}, nil
	}

	fileHandle, fileErr := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": false}))
	if fileErr == nil {
		f.cache.invalidate(name)

		if truncate && !readonly {
			writable, err := awaitErr(fileHandle.Call("createWritable", map[string]any{"keepExistingData": false}))
			if err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
			if _, err := awaitErr(writable.Call("close")); err != nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
			}
		}

		return &localdirFile{
			fsys:     f,
			name:     name,
			handle:   fileHandle,
			readonly: readonly,
		}, nil
	}

	dirHandle2, dirErr := awaitErr(dirHandle.Call("getDirectoryHandle", baseName, map[string]any{"create": false}))
	if dirErr == nil {
		return &localdirFile{
			fsys:  f,
			name:  name,
			handle: dirHandle2,
			isDir: true,
		}, nil
	}

	if isNotFound(fileErr) || isNotFound(dirErr) {
		return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: hackpadfs.ErrNotExist}
	}
	return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: fileErr}
}

func (f *FS) openFile(name string, create bool) (hackpadfs.File, error) {
	return f.OpenFile(name, hackpadfs.FlagReadOnly, 0)
}

// Stat implements hackpadfs.StatFS.
func (f *FS) Stat(name string) (hackpadfs.FileInfo, error) {
	if info, err, ok := f.cache.get(name); ok {
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	info, err := f.stat(name)
	if err != nil {
		f.cache.setErr(name, err)
		return nil, err
	}
	f.cache.set(name, info)
	return info, nil
}

func (f *FS) stat(name string) (hackpadfs.FileInfo, error) {
	if name == "." {
		return f.statFile(name, f.root)
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return nil, err
	}
	baseName := path.Base(name)

	fileHandle, fileErr := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": false}))
	if fileErr == nil {
		return f.statFile(name, fileHandle)
	}

	dirHandle2, dirErr := awaitErr(dirHandle.Call("getDirectoryHandle", baseName, map[string]any{"create": false}))
	if dirErr == nil {
		return f.statFile(name, dirHandle2)
	}

	if isNotFound(fileErr) || isNotFound(dirErr) {
		return nil, hackpadfs.ErrNotExist
	}
	return nil, fileErr
}

// statFile builds a hackpadfs.FileInfo from a JS handle and metadata store.
func (f *FS) statFile(name string, handle js.Value) (hackpadfs.FileInfo, error) {
	isDir := handle.Get("kind").String() == "directory"

	var size int64
	var jsMtime time.Time

	if isDir {
		size = 0
		jsMtime = time.Now()
	} else {
		file, err := awaitErr(handle.Call("getFile"))
		if err != nil {
			return nil, err
		}
		size = int64(file.Get("size").Float())
		jsMtime = time.UnixMilli(int64(file.Get("lastModified").Int()))
	}

	meta, hasMeta := f.meta.get(name)
	var mode hackpadfs.FileMode
	var mtime, atime time.Time

	if hasMeta {
		mode = meta.Mode
		mtime = meta.Mtime
		atime = meta.Atime
	} else {
		if isDir {
			mode = 0755 | hackpadfs.ModeDir
		} else {
			mode = 0644
		}
		mtime = jsMtime
		atime = time.Now()
		f.meta.set(name, fileMetadata{Mode: mode, Mtime: mtime, Atime: atime})
	}

	if isDir {
		mode |= hackpadfs.ModeDir
	}

	return &localdirFileInfo{
		name:  path.Base(name),
		size:  size,
		mode:  mode,
		mtime: mtime,
		isDir: isDir,
	}, nil
}

// localdirFileInfo implements hackpadfs.FileInfo and hackpadfs.DirEntry.
type localdirFileInfo struct {
	name  string
	size  int64
	mode  hackpadfs.FileMode
	mtime time.Time
	isDir bool
}

func (fi *localdirFileInfo) Name() string                      { return fi.name }
func (fi *localdirFileInfo) Size() int64                       { return fi.size }
func (fi *localdirFileInfo) Mode() hackpadfs.FileMode          { return fi.mode }
func (fi *localdirFileInfo) ModTime() time.Time                { return fi.mtime }
func (fi *localdirFileInfo) IsDir() bool                       { return fi.isDir }
func (fi *localdirFileInfo) Sys() interface{}                  { return nil }
func (fi *localdirFileInfo) Info() (hackpadfs.FileInfo, error) { return fi, nil }
func (fi *localdirFileInfo) Type() hackpadfs.FileMode          { return fi.mode.Type() }

// Mkdir implements hackpadfs.MkdirFS.
func (f *FS) Mkdir(name string, perm hackpadfs.FileMode) error {
	if err := f.ensureWritePermission(); err != nil {
		return &hackpadfs.PathError{Op: "mkdir", Path: name, Err: err}
	}
	if name == "." {
		return nil
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return &hackpadfs.PathError{Op: "mkdir", Path: name, Err: hackpadfs.ErrNotExist}
	}
	_, err = awaitErr(dirHandle.Call("getDirectoryHandle", path.Base(name), map[string]any{"create": true}))
	if err != nil {
		return &hackpadfs.PathError{Op: "mkdir", Path: name, Err: err}
	}
	f.meta.set(name, fileMetadata{Mode: perm | hackpadfs.ModeDir, Mtime: time.Now(), Atime: time.Now()})
	f.cache.invalidate(name)
	return nil
}

// MkdirAll implements hackpadfs.MkdirAllFS.
func (f *FS) MkdirAll(pathStr string, perm hackpadfs.FileMode) error {
	if err := f.ensureWritePermission(); err != nil {
		return &hackpadfs.PathError{Op: "mkdir", Path: pathStr, Err: err}
	}
	if pathStr == "." {
		return nil
	}
	parts := strings.Split(strings.Trim(pathStr, "/"), "/")
	cur := f.root
	accum := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if accum != "" {
			accum += "/"
		}
		accum += part
		var err error
		cur, err = awaitErr(cur.Call("getDirectoryHandle", part, map[string]any{"create": true}))
		if err != nil {
			return &hackpadfs.PathError{Op: "mkdir", Path: accum, Err: err}
		}
		f.meta.set(accum, fileMetadata{Mode: perm | hackpadfs.ModeDir, Mtime: time.Now(), Atime: time.Now()})
		f.cache.invalidate(accum)
	}
	return nil
}

// Remove implements hackpadfs.RemoveFS.
func (f *FS) Remove(name string) error {
	if err := f.ensureWritePermission(); err != nil {
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: err}
	}
	if name == "." {
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrInvalid}
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrNotExist}
	}
	_, err = awaitErr(dirHandle.Call("removeEntry", path.Base(name)))
	if err != nil {
		if isNotFound(err) {
			return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrNotExist}
		}
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: err}
	}
	f.meta.del(name)
	f.cache.invalidate(name)
	return nil
}

// RemoveAll implements hackpadfs.RemoveAllFS.
func (f *FS) RemoveAll(name string) error {
	if err := f.ensureWritePermission(); err != nil {
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: err}
	}
	if name == "." {
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrInvalid}
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrNotExist}
	}
	_, err = awaitErr(dirHandle.Call("removeEntry", path.Base(name), map[string]any{"recursive": true}))
	if err != nil {
		if isNotFound(err) {
			return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrNotExist}
		}
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: err}
	}
	f.meta.delPrefix(name)
	f.cache.invalidatePrefix(name)
	return nil
}

// Rename implements hackpadfs.RenameFS.
// The File System Access API doesn't support native rename, so we copy + delete.
func (f *FS) Rename(oldname, newname string) error {
	if err := f.ensureWritePermission(); err != nil {
		return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
	}
	info, err := f.Stat(oldname)
	if err != nil {
		return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
	}

	if _, err := f.Stat(newname); err == nil {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: hackpadfs.ErrExist}
	} else if !errors.Is(err, hackpadfs.ErrNotExist) {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
	}

	if err := f.MkdirAll(path.Dir(newname), 0755); err != nil {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
	}

	if info.IsDir() {
		if err := f.Mkdir(newname, info.Mode().Perm()); err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
		}
		dir, err := f.Open(oldname)
		if err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
		}
		entries, err := dir.(hackpadfs.DirReaderFile).ReadDir(-1)
		dir.Close()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			childOld := path.Join(oldname, entry.Name())
			childNew := path.Join(newname, entry.Name())
			if err := f.Rename(childOld, childNew); err != nil {
				return err
			}
		}
		if meta, ok := f.meta.get(oldname); ok {
			f.meta.set(newname, meta)
		}
		if err := f.Remove(oldname); err != nil {
			return err
		}
	} else {
		srcFile, err := f.Open(oldname)
		if err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
		}
		defer srcFile.Close()

		dstFile, err := f.OpenFile(newname, hackpadfs.FlagCreate|hackpadfs.FlagTruncate|hackpadfs.FlagWriteOnly, 0644)
		if err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
		}

		buf := make([]byte, 32768)
		for {
			n, readErr := srcFile.Read(buf)
			if n > 0 {
				if _, writeErr := dstFile.(hackpadfs.ReadWriterFile).Write(buf[:n]); writeErr != nil {
					dstFile.Close()
					return &hackpadfs.PathError{Op: "rename", Path: newname, Err: writeErr}
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					dstFile.Close()
					return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: readErr}
				}
				break
			}
		}
		dstFile.Close()

		if meta, ok := f.meta.get(oldname); ok {
			f.meta.set(newname, meta)
		}
		if err := f.Remove(oldname); err != nil {
			return err
		}
	}

	f.cache.invalidate(oldname)
	f.cache.invalidate(newname)
	return nil
}

// Chmod implements hackpadfs.ChmodFS.
func (f *FS) Chmod(name string, mode hackpadfs.FileMode) error {
	f.meta.setMode(name, mode)
	return nil
}

// Chtimes implements hackpadfs.ChtimesFS.
func (f *FS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	f.meta.setTimes(name, atime, mtime)
	return nil
}

// walkDir traverses a path and returns the directory handle.
func (f *FS) walkDir(dirPath string) (js.Value, error) {
	if dirPath == "." {
		return f.root, nil
	}
	dirPath = strings.Trim(dirPath, "/")
	parts := strings.Split(dirPath, "/")
	cur := f.root
	for _, part := range parts {
		if part == "" {
			continue
		}
		var err error
		cur, err = awaitErr(cur.Call("getDirectoryHandle", part, map[string]any{"create": false}))
		if err != nil {
			return js.Undefined(), err
		}
	}
	return cur, nil
}

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

func isNotFound(err error) bool {
	if jsErr, ok := err.(js.Error); ok {
		return jsErr.Value.Get("name").String() == "NotFoundError"
	}
	return false
}

// compile-time interface checks
var _ hackpadfs.FS = (*FS)(nil)
var _ hackpadfs.OpenFileFS = (*FS)(nil)
var _ hackpadfs.MkdirFS = (*FS)(nil)
var _ hackpadfs.MkdirAllFS = (*FS)(nil)
var _ hackpadfs.RemoveFS = (*FS)(nil)
var _ hackpadfs.RemoveAllFS = (*FS)(nil)
var _ hackpadfs.RenameFS = (*FS)(nil)
var _ hackpadfs.StatFS = (*FS)(nil)
var _ hackpadfs.ChmodFS = (*FS)(nil)
var _ hackpadfs.ChtimesFS = (*FS)(nil)
