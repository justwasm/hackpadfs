//go:build js

package opfs

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"github.com/hack-pad/hackpadfs"
)

// OPFS implements hackpadfs.FS backed by the browser's Origin Private File System (OPFS).
// It satisfies the interfaces needed for hackpad's persistent storage:
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
//   - clearFS (Clear)
type OPFS struct {
	root  js.Value // FileSystemDirectoryHandle (OPFS root)
	meta  *metadataStore
	cache *statCache
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

const statCacheMax = 1000

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
	if len(c.entries) >= statCacheMax {
		c.purgeExpired()
	}
	c.entries[key] = &statCacheEntry{info: info, cachedAt: time.Now()}
}

func (c *statCache) setErr(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= statCacheMax {
		c.purgeExpired()
	}
	c.entries[key] = &statCacheEntry{err: err, cachedAt: time.Now()}
}

func (c *statCache) purgeExpired() {
	now := time.Now()
	for k, e := range c.entries {
		if now.Sub(e.cachedAt) > c.ttl {
			delete(c.entries, k)
		}
	}
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

func (c *statCache) invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// NewOPFS creates and initializes an OPFS-backed filesystem.
// If namespace is provided, the FS is scoped to a subdirectory of the OPFS root
// for isolation (like separate IndexedDB databases per mount).
// The namespace is treated as a relative path (leading/trailing slashes stripped).
func NewOPFS(namespace ...string) (*OPFS, error) {
	rootDir, err := awaitErr(js.Global().Get("navigator").Get("storage").Call("getDirectory"))
	if err != nil {
		return nil, err
	}

	if len(namespace) > 0 && namespace[0] != "" {
		parts := strings.Split(strings.Trim(namespace[0], "/"), "/")
		for _, part := range parts {
			if part == "" {
				continue
			}
			rootDir, err = awaitErr(rootDir.Call("getDirectoryHandle", part, map[string]any{"create": true}))
			if err != nil {
				return nil, err
			}
		}
	}

	fsys := &OPFS{
		root:  rootDir,
		meta:  newMetadataStore(),
		cache: newStatCache(),
	}

	if err := fsys.meta.load(rootDir); err != nil {
		return nil, err
	}

	return fsys, nil
}

// Open implements hackpadfs.FS.
func (f *OPFS) Open(name string) (hackpadfs.File, error) {
	return f.openFile(name, false)
}

// OpenFile implements hackpadfs.OpenFileFS.
// Follows symlinks: if the opened path is a symlink, opens the resolved target instead.
func (f *OPFS) OpenFile(name string, flag int, perm hackpadfs.FileMode) (hackpadfs.File, error) {
	// First resolve symlinks (but not for create path — new files aren't symlinks)
	if flag&hackpadfs.FlagCreate == 0 && flag&hackpadfs.FlagAppend == 0 {
		resolved, err := f.resolveSymlink(name, 0)
		if err != nil {
			return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: err}
		}
		if resolved != name {
			return f.OpenFile(resolved, flag, perm)
		}
	}

	create := flag&hackpadfs.FlagCreate != 0 || flag&hackpadfs.FlagAppend != 0
	readonly := flag&hackpadfs.FlagWriteOnly == 0 && flag&hackpadfs.FlagReadWrite == 0
	truncate := flag&hackpadfs.FlagTruncate != 0
	appendMode := flag&hackpadfs.FlagAppend != 0

	if name == "." {
		dir := &opfsFile{
			fsys:   f,
			name:   ".",
			handle: f.root,
			isDir:  true,
		}
		return dir, nil
	}

	dirPath := path.Dir(name)
	baseName := path.Base(name)

	dirHandle, err := f.walkDir(dirPath)
	if err != nil {
		if create {
			// Auto-create parent dirs
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
		// O_EXCL: fail if file already exists
		if flag&hackpadfs.FlagExclusive != 0 {
			if _, err := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": false})); err == nil {
				return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: hackpadfs.ErrExist}
			} else if !isOPFSNotFound(err) {
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

		return &opfsFile{
			fsys:     f,
			name:     name,
			handle:   fileHandle,
			append:   appendMode,
			readonly: readonly,
			offset:   writeOffset,
		}, nil
	}

	// Try file first, then directory
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

		return &opfsFile{
			fsys:     f,
			name:     name,
			handle:   fileHandle,
			readonly: readonly,
		}, nil
	}

	dirHandle2, dirErr := awaitErr(dirHandle.Call("getDirectoryHandle", baseName, map[string]any{"create": false}))
	if dirErr == nil {
		return &opfsFile{
			fsys:   f,
			name:   name,
			handle: dirHandle2,
			isDir:  true,
		}, nil
	}

	if isOPFSNotFound(fileErr) || isOPFSNotFound(dirErr) {
		return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: hackpadfs.ErrNotExist}
	}
	return nil, &hackpadfs.PathError{Op: "open", Path: name, Err: fileErr}
}

func (f *OPFS) openFile(name string, create bool) (hackpadfs.File, error) {
	return f.OpenFile(name, hackpadfs.FlagReadOnly, 0)
}

// Stat implements hackpadfs.StatFS. Follows symlinks to the target.
func (f *OPFS) Stat(name string) (hackpadfs.FileInfo, error) {
	resolved, err := f.resolveSymlink(name, 0)
	if err != nil {
		return nil, &hackpadfs.PathError{Op: "stat", Path: name, Err: err}
	}
	return f.statUncached(resolved)
}

// Lstat implements hackpadfs.LstatFS. Returns the symlink's own info without following.
func (f *OPFS) Lstat(name string) (hackpadfs.FileInfo, error) {
	if info, err, ok := f.cache.get(name); ok {
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	info, err := f.statNoFollow(name)
	if err != nil {
		f.cache.setErr(name, err)
		return nil, err
	}
	f.cache.set(name, info)
	return info, nil
}

// statNoFollow builds FileInfo from the raw OPFS handle without symlink following.
func (f *OPFS) statNoFollow(name string) (hackpadfs.FileInfo, error) {
	if name == "." {
		return f.statFile(name, f.root)
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return nil, err
	}
	baseName := path.Base(name)

	// Try file first, then directory (entry may be either)
	fileHandle, fileErr := awaitErr(dirHandle.Call("getFileHandle", baseName, map[string]any{"create": false}))
	if fileErr == nil {
		return f.statFile(name, fileHandle)
	}

	dirHandle2, dirErr := awaitErr(dirHandle.Call("getDirectoryHandle", baseName, map[string]any{"create": false}))
	if dirErr == nil {
		return f.statFile(name, dirHandle2)
	}

	// Neither worked — return the original error if it's a real error,
	// or return not found if the entry simply doesn't exist.
	if isOPFSNotFound(fileErr) || isOPFSNotFound(dirErr) {
		return nil, hackpadfs.ErrNotExist
	}
	return nil, fileErr
}

func (f *OPFS) statUncached(name string) (hackpadfs.FileInfo, error) {
	if info, err, ok := f.cache.get(name); ok {
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	file, err := f.Open(name)
	if err != nil {
		f.cache.setErr(name, err)
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		f.cache.setErr(name, err)
		return nil, &hackpadfs.PathError{Op: "stat", Path: name, Err: err}
	}
	f.cache.set(name, info)
	return info, nil
}

// Mkdir implements hackpadfs.MkdirFS.
func (f *OPFS) Mkdir(name string, perm hackpadfs.FileMode) error {
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
func (f *OPFS) MkdirAll(pathStr string, perm hackpadfs.FileMode) error {
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
func (f *OPFS) Remove(name string) error {
	if name == "." {
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrInvalid}
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrNotExist}
	}
	_, err = awaitErr(dirHandle.Call("removeEntry", path.Base(name)))
	if err != nil {
		if isOPFSNotFound(err) {
			return &hackpadfs.PathError{Op: "remove", Path: name, Err: hackpadfs.ErrNotExist}
		}
		return &hackpadfs.PathError{Op: "remove", Path: name, Err: err}
	}
	f.meta.del(name)
	f.cache.invalidate(name)
	return nil
}

// RemoveAll implements hackpadfs.RemoveAllFS.
func (f *OPFS) RemoveAll(name string) error {
	if name == "." {
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrInvalid}
	}
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrNotExist}
	}
	_, err = awaitErr(dirHandle.Call("removeEntry", path.Base(name), map[string]any{"recursive": true}))
	if err != nil {
		if isOPFSNotFound(err) {
			return &hackpadfs.PathError{Op: "removeall", Path: name, Err: hackpadfs.ErrNotExist}
		}
		return &hackpadfs.PathError{Op: "removeall", Path: name, Err: err}
	}
	f.meta.delPrefix(name)
	f.cache.invalidatePrefix(name)
	return nil
}

// Rename implements hackpadfs.RenameFS.
// OPFS doesn't support native rename, so we copy + delete.
func (f *OPFS) Rename(oldname, newname string) error {
	info, err := f.Stat(oldname)
	if err != nil {
		return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
	}

	// Check destination doesn't already exist (prevent silent merge)
	if _, err := f.Stat(newname); err == nil {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: hackpadfs.ErrExist}
	} else if !errors.Is(err, hackpadfs.ErrNotExist) {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
	}

	// Create parent directory for destination
	if err := f.MkdirAll(path.Dir(newname), 0755); err != nil {
		return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
	}

	if info.IsDir() {
		// Create destination directory with same mode
		if err := f.Mkdir(newname, info.Mode().Perm()); err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
		}

		// Recursively rename children
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

		// Copy metadata
		if meta, ok := f.meta.get(oldname); ok {
			f.meta.set(newname, meta)
		}

		// Remove old directory (should be empty now)
		if err := f.Remove(oldname); err != nil {
			return err
		}
	} else {
		srcFile, err := f.Open(oldname)
		if err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: oldname, Err: err}
		}
		defer srcFile.Close()

		// Create destination file (truncate to handle retry with smaller content)
		dstFile, err := f.OpenFile(newname, hackpadfs.FlagCreate|hackpadfs.FlagTruncate|hackpadfs.FlagWriteOnly, 0644)
		if err != nil {
			return &hackpadfs.PathError{Op: "rename", Path: newname, Err: err}
		}

		// Copy contents
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

		// Copy metadata
		if meta, ok := f.meta.get(oldname); ok {
			f.meta.set(newname, meta)
		}

		// Remove old file
		if err := f.Remove(oldname); err != nil {
			return err
		}
	}

	f.cache.invalidate(oldname)
	f.cache.invalidate(newname)
	return nil
}

// Chmod implements hackpadfs.ChmodFS.
func (f *OPFS) Chmod(name string, mode hackpadfs.FileMode) error {
	f.meta.setMode(name, mode)
	return nil
}

// Chtimes implements hackpadfs.ChtimesFS.
func (f *OPFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	f.meta.setTimes(name, atime, mtime)
	return nil
}

// Clear removes all files from the OPFS filesystem, including metadata.
func (f *OPFS) Clear(ctx context.Context) error {
	// Remove all entries from OPFS root
	iter := f.root.Call("values")
	var res js.Value
	var err error
	res, err = awaitErr(iter.Call("next"))
	if err != nil {
		return err
	}
	for !res.Get("done").Bool() {
		val := res.Get("value")
		name := val.Get("name").String()
		_, err = awaitErr(f.root.Call("removeEntry", name, map[string]any{"recursive": true}))
		if err != nil {
			return err
		}
		res, err = awaitErr(iter.Call("next"))
		if err != nil {
			return err
		}
	}
	// Start fresh: reset metadata store in-place (avoids leaking writeLoop goroutine)
	f.meta.reset(f.root)
	f.cache = newStatCache()
	return nil
}

// walkDir traverses a path and returns the directory handle.
func (f *OPFS) walkDir(dirPath string) (js.Value, error) {
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

// statFile builds a hackpadfs.FileInfo from a JS handle and metadata store.
func (f *OPFS) statFile(name string, handle js.Value) (hackpadfs.FileInfo, error) {
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

	return &opfsFileInfo{
		name:  path.Base(name),
		size:  size,
		mode:  mode,
		mtime: mtime,
		isDir: isDir,
	}, nil
}

// opfsFileInfo implements hackpadfs.FileInfo and hackpadfs.DirEntry.
type opfsFileInfo struct {
	name  string
	size  int64
	mode  hackpadfs.FileMode
	mtime time.Time
	isDir bool
}

func (fi *opfsFileInfo) Name() string                      { return fi.name }
func (fi *opfsFileInfo) Size() int64                       { return fi.size }
func (fi *opfsFileInfo) Mode() hackpadfs.FileMode          { return fi.mode }
func (fi *opfsFileInfo) ModTime() time.Time                { return fi.mtime }
func (fi *opfsFileInfo) IsDir() bool                       { return fi.isDir }
func (fi *opfsFileInfo) Sys() interface{}                  { return nil }
func (fi *opfsFileInfo) Info() (hackpadfs.FileInfo, error) { return fi, nil }
func (fi *opfsFileInfo) Type() hackpadfs.FileMode          { return fi.mode.Type() }

// Symlink implements hackpadfs.SymlinkFS.
// Stores the target as file content with ModeSymlink in metadata.
// Returns ErrExist if newname already exists.
func (f *OPFS) Symlink(oldname, newname string) error {
	// Check if target already exists
	if _, err := f.Stat(newname); err == nil {
		return &hackpadfs.PathError{Op: "symlink", Path: newname, Err: hackpadfs.ErrExist}
	} else if !errors.Is(err, hackpadfs.ErrNotExist) {
		return &hackpadfs.PathError{Op: "symlink", Path: newname, Err: err}
	}
	dirPath := path.Dir(newname)
	if err := f.MkdirAll(dirPath, 0755); err != nil {
		return &hackpadfs.PathError{Op: "symlink", Path: newname, Err: err}
	}
	file, err := f.OpenFile(newname, hackpadfs.FlagCreate|hackpadfs.FlagExclusive|hackpadfs.FlagWriteOnly, 0777)
	if err != nil {
		return &hackpadfs.PathError{Op: "symlink", Path: newname, Err: err}
	}
	if _, err := file.(hackpadfs.ReadWriterFile).Write([]byte(oldname)); err != nil {
		file.Close()
		return &hackpadfs.PathError{Op: "symlink", Path: newname, Err: err}
	}
	file.Close()
	f.meta.setMode(newname, hackpadfs.FileMode(0777)|hackpadfs.ModeSymlink)
	f.cache.invalidate(newname)
	return nil
}

// Readlink implements hackpadfs.ReadlinkFS.
func (f *OPFS) Readlink(name string) (string, error) {
	data, err := f.readRawFile(name)
	if err != nil {
		return "", &hackpadfs.PathError{Op: "readlink", Path: name, Err: err}
	}
	if len(data) == 0 {
		return "", &hackpadfs.PathError{Op: "readlink", Path: name, Err: hackpadfs.ErrInvalid}
	}
	return string(data), nil
}

// resolveSymlink follows a symlink chain up to 40 levels deep.
// Returns the resolved path (or the original if not a symlink).
func (f *OPFS) resolveSymlink(name string, depth int) (string, error) {
	if depth > 40 {
		return "", hackpadfs.ErrInvalid
	}
	meta, ok := f.meta.get(name)
	if !ok || meta.Mode&hackpadfs.ModeSymlink == 0 {
		return name, nil
	}
	data, err := f.readRawFile(name)
	if err != nil {
		return "", err
	}
	target := strings.TrimSpace(string(data))
	if target == "" {
		return "", &hackpadfs.PathError{Op: "readlink", Path: name, Err: hackpadfs.ErrInvalid}
	}
	// Resolve relative targets against the symlink's parent directory
	if !path.IsAbs(target) {
		target = path.Join(path.Dir(name), target)
	} else {
		target = strings.TrimPrefix(target, "/")
	}
	return f.resolveSymlink(target, depth+1)
}

// readRawFile reads file content from OPFS without following symlinks.
func (f *OPFS) readRawFile(name string) ([]byte, error) {
	dirHandle, err := f.walkDir(path.Dir(name))
	if err != nil {
		return nil, err
	}
	fileHandle, err := awaitErr(dirHandle.Call("getFileHandle", path.Base(name), map[string]any{"create": false}))
	if err != nil {
		return nil, err
	}
	file, err := awaitErr(fileHandle.Call("getFile"))
	if err != nil {
		return nil, err
	}
	buf, err := awaitErr(file.Call("arrayBuffer"))
	if err != nil {
		return nil, err
	}
	jsBuf := js.Global().Get("Uint8Array").New(buf)
	data := make([]byte, jsBuf.Length())
	js.CopyBytesToGo(data, jsBuf)
	return data, nil
}

// Chown implements hackpadfs.ChownFS.
// OPFS does not support ownership natively; metadata is stored but returns no error.
func (f *OPFS) Chown(name string, uid, gid int) error {
	f.meta.setChown(name, uid, gid)
	return nil
}

// compile-time interface checks
var _ hackpadfs.FS = (*OPFS)(nil)
var _ hackpadfs.OpenFileFS = (*OPFS)(nil)
var _ hackpadfs.MkdirFS = (*OPFS)(nil)
var _ hackpadfs.MkdirAllFS = (*OPFS)(nil)
var _ hackpadfs.RemoveFS = (*OPFS)(nil)
var _ hackpadfs.RemoveAllFS = (*OPFS)(nil)
var _ hackpadfs.RenameFS = (*OPFS)(nil)
var _ hackpadfs.StatFS = (*OPFS)(nil)
var _ hackpadfs.LstatFS = (*OPFS)(nil)
var _ hackpadfs.ChmodFS = (*OPFS)(nil)
var _ hackpadfs.ChownFS = (*OPFS)(nil)
var _ hackpadfs.ChtimesFS = (*OPFS)(nil)
var _ hackpadfs.SymlinkFS = (*OPFS)(nil)
var _ hackpadfs.ReadlinkFS = (*OPFS)(nil)
