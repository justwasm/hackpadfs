//go:build js

package localdir

import (
	"io"
	"path"
	"sync"
	"syscall/js"
	"time"

	"github.com/hack-pad/hackpadfs"
)

// localdirFile implements hackpadfs.File for local File System Access API handles.
// It supports Read, Write, Seek, Truncate, Stat, Sync, and ReadDir.
type localdirFile struct {
	fsys   *FS
	name   string
	handle js.Value // FileSystemFileHandle or FileSystemDirectoryHandle
	isDir  bool

	mu       sync.Mutex
	offset   int64
	closed   bool
	file     js.Value // lazily fetched File (from getFile())
	writer   js.Value // lazily created WritableFileStream
	append   bool
	readonly bool

	writeMu sync.Mutex // serializes all writer operations
}

var (
	_ hackpadfs.File           = (*localdirFile)(nil)
	_ hackpadfs.ReadWriterFile = (*localdirFile)(nil)
	_ hackpadfs.SeekerFile     = (*localdirFile)(nil)
	_ hackpadfs.TruncaterFile  = (*localdirFile)(nil)
	_ hackpadfs.SyncerFile     = (*localdirFile)(nil)
	_ hackpadfs.DirReaderFile  = (*localdirFile)(nil)
)

func (f *localdirFile) Stat() (hackpadfs.FileInfo, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, hackpadfs.ErrClosed
	}
	if !f.writer.IsUndefined() {
		writer := f.writer
		f.writer = js.Undefined()
		f.file = js.Undefined()
		f.mu.Unlock()
		_, err := awaitErr(writer.Call("close"))
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
	}
	f.mu.Unlock()

	return f.fsys.statFile(f.name, f.handle)
}

func (f *localdirFile) Read(p []byte) (int, error) {
	return f.readAt(p, -1)
}

func (f *localdirFile) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(p, off)
}

func (f *localdirFile) readAt(p []byte, off int64) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, hackpadfs.ErrClosed
	}
	if f.isDir {
		f.mu.Unlock()
		return 0, &hackpadfs.PathError{Op: "read", Path: f.name, Err: hackpadfs.ErrIsDir}
	}
	if !f.writer.IsUndefined() {
		writer := f.writer
		f.writer = js.Undefined()
		f.file = js.Undefined()
		f.mu.Unlock()
		_, err := awaitErr(writer.Call("close"))
		if err != nil {
			return 0, err
		}
		f.mu.Lock()
	}
	if f.file.IsUndefined() {
		file, err := awaitErr(f.handle.Call("getFile"))
		if err != nil {
			f.mu.Unlock()
			return 0, err
		}
		f.file = file
	}
	useOffset := off < 0
	if useOffset {
		off = f.offset
	}
	f.mu.Unlock()

	size := int64(f.file.Get("size").Float())
	if off >= size {
		return 0, io.EOF
	}

	end := off + int64(len(p))
	if end > size {
		end = size
	}

	blobVal := f.file.Call("slice", off, end)
	arrBuf, err := awaitErr(blobVal.Call("arrayBuffer"))
	if err != nil {
		return 0, err
	}

	jsBuf := js.Global().Get("Uint8Array").New(arrBuf)
	data := make([]byte, jsBuf.Length())
	n := js.CopyBytesToGo(data, jsBuf)
	copy(p, data)

	if useOffset {
		f.mu.Lock()
		f.offset += int64(n)
		f.mu.Unlock()
	}

	return n, nil
}

func (f *localdirFile) Write(p []byte) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, hackpadfs.ErrClosed
	}
	if f.isDir {
		f.mu.Unlock()
		return 0, &hackpadfs.PathError{Op: "write", Path: f.name, Err: hackpadfs.ErrIsDir}
	}
	if f.readonly {
		f.mu.Unlock()
		return 0, &hackpadfs.PathError{Op: "write", Path: f.name, Err: hackpadfs.ErrPermission}
	}
	if f.writer.IsUndefined() {
		if f.append {
			file, err := awaitErr(f.handle.Call("getFile"))
			if err == nil {
				f.offset = int64(file.Get("size").Int())
			}
		}
		writer, err := awaitErr(f.handle.Call("createWritable", map[string]any{"keepExistingData": true}))
		if err != nil {
			f.mu.Unlock()
			return 0, err
		}
		f.writer = writer
	} else if f.append {
		existingWriter := f.writer
		f.writer = js.Undefined()
		if _, err := awaitErr(existingWriter.Call("close")); err != nil {
			f.mu.Unlock()
			return 0, err
		}
		newWriter, err := awaitErr(f.handle.Call("createWritable", map[string]any{"keepExistingData": true}))
		if err != nil {
			f.mu.Unlock()
			return 0, err
		}
		f.writer = newWriter
		file, err := awaitErr(f.handle.Call("getFile"))
		if err == nil {
			f.offset = int64(file.Get("size").Float())
		}
	}
	offset := f.offset
	writer := f.writer
	f.mu.Unlock()

	jsBuf := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(jsBuf, p)

	_, err := awaitErr(writer.Call("write", map[string]any{
		"type":     "write",
		"data":     jsBuf,
		"position": offset,
	}))
	if err != nil {
		return 0, err
	}

	f.mu.Lock()
	f.offset += int64(len(p))
	f.mu.Unlock()

	f.fsys.meta.setTimes(f.name, time.Now(), time.Now())
	f.fsys.cache.invalidate(f.name)
	return len(p), nil
}

func (f *localdirFile) WriteAt(p []byte, off int64) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, hackpadfs.ErrClosed
	}
	if f.isDir {
		f.mu.Unlock()
		return 0, &hackpadfs.PathError{Op: "write", Path: f.name, Err: hackpadfs.ErrIsDir}
	}
	if f.readonly {
		f.mu.Unlock()
		return 0, &hackpadfs.PathError{Op: "write", Path: f.name, Err: hackpadfs.ErrPermission}
	}
	if f.writer.IsUndefined() {
		writer, err := awaitErr(f.handle.Call("createWritable", map[string]any{"keepExistingData": true}))
		if err != nil {
			f.mu.Unlock()
			return 0, err
		}
		f.writer = writer
	}
	writer := f.writer
	f.mu.Unlock()

	jsBuf := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(jsBuf, p)

	_, err := awaitErr(writer.Call("write", map[string]any{
		"type":     "write",
		"data":     jsBuf,
		"position": off,
	}))
	if err != nil {
		return 0, err
	}

	f.fsys.meta.setTimes(f.name, time.Now(), time.Now())
	f.fsys.cache.invalidate(f.name)
	return len(p), nil
}

func (f *localdirFile) Close() error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return hackpadfs.ErrClosed
	}
	f.closed = true
	writer := f.writer
	f.file = js.Undefined()
	f.writer = js.Undefined()
	f.mu.Unlock()

	if !writer.IsUndefined() {
		_, err := awaitErr(writer.Call("close"))
		return err
	}
	return nil
}

func (f *localdirFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, hackpadfs.ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		if f.file.IsUndefined() {
			file, err := awaitErr(f.handle.Call("getFile"))
			if err != nil {
				return 0, err
			}
			f.file = file
		}
		newOffset = int64(f.file.Get("size").Float()) + offset
	default:
		return 0, &hackpadfs.PathError{Op: "seek", Path: f.name, Err: hackpadfs.ErrInvalid}
	}
	if newOffset < 0 {
		return 0, &hackpadfs.PathError{Op: "seek", Path: f.name, Err: hackpadfs.ErrInvalid}
	}
	f.offset = newOffset
	return newOffset, nil
}

func (f *localdirFile) Truncate(size int64) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return hackpadfs.ErrClosed
	}
	if f.isDir {
		f.mu.Unlock()
		return &hackpadfs.PathError{Op: "truncate", Path: f.name, Err: hackpadfs.ErrIsDir}
	}
	if f.readonly {
		f.mu.Unlock()
		return &hackpadfs.PathError{Op: "truncate", Path: f.name, Err: hackpadfs.ErrPermission}
	}
	if f.writer.IsUndefined() {
		writer, err := awaitErr(f.handle.Call("createWritable", map[string]any{"keepExistingData": true}))
		if err != nil {
			f.mu.Unlock()
			return err
		}
		f.writer = writer
	}
	writer := f.writer
	f.mu.Unlock()

	_, err := awaitErr(writer.Call("write", map[string]any{
		"type": "truncate",
		"size": size,
	}))
	if err == nil {
		f.fsys.meta.setTimes(f.name, time.Now(), time.Now())
		f.fsys.cache.invalidate(f.name)
	}
	return err
}

func (f *localdirFile) Sync() error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	writer := f.writer
	f.mu.Unlock()

	if !writer.IsUndefined() {
		_, err := awaitErr(writer.Call("close"))
		if err != nil {
			return err
		}
		f.mu.Lock()
		newWriter, err := awaitErr(f.handle.Call("createWritable", map[string]any{"keepExistingData": true}))
		if err != nil {
			f.mu.Unlock()
			return err
		}
		f.writer = newWriter
		f.mu.Unlock()
	}
	return nil
}

func (f *localdirFile) ReadDir(n int) ([]hackpadfs.DirEntry, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, hackpadfs.ErrClosed
	}
	if !f.isDir {
		f.mu.Unlock()
		return nil, &hackpadfs.PathError{Op: "readdir", Path: f.name, Err: hackpadfs.ErrNotDir}
	}
	f.mu.Unlock()

	var entries []hackpadfs.DirEntry
	iter := f.handle.Call("values")

	var res js.Value
	var err error
	res, err = awaitErr(iter.Call("next"))
	if err != nil {
		return nil, err
	}
	for !res.Get("done").Bool() {
		val := res.Get("value")
		entryName := val.Get("name").String()
		entryPath := path.Join(f.name, entryName)

		info, err := f.fsys.statFile(entryPath, val)
		if err != nil {
			return nil, err
		}
		entries = append(entries, info.(hackpadfs.DirEntry))

		res, err = awaitErr(iter.Call("next"))
		if err != nil {
			return nil, err
		}
	}

	if n >= 0 && len(entries) > n {
		entries = entries[:n]
	}

	if entries == nil {
		entries = []hackpadfs.DirEntry{}
	}
	return entries, nil
}
