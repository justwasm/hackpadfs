//go:build js

package opfs

import (
	"io"
	"path"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"github.com/hack-pad/hackpadfs"
	"github.com/hack-pad/hackpadfs/keyvalue/blob"
)

// opfsFile implements hackpadfs.File for OPFS file handles.
// It supports Read, Write, Seek, Truncate, Stat, Sync, and Chmod/Chtimes.
type opfsFile struct {
	fsys   *OPFS
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
	_ hackpadfs.File           = (*opfsFile)(nil)
	_ hackpadfs.ReadWriterFile = (*opfsFile)(nil)
	_ hackpadfs.SeekerFile     = (*opfsFile)(nil)
	_ hackpadfs.TruncaterFile  = (*opfsFile)(nil)
	_ hackpadfs.SyncerFile     = (*opfsFile)(nil)
	_ hackpadfs.DirReaderFile  = (*opfsFile)(nil)
	_ hackpadfs.ChmoderFile    = (*opfsFile)(nil)
	_ hackpadfs.ChownerFile    = (*opfsFile)(nil)
	_ hackpadfs.ChtimeserFile  = (*opfsFile)(nil)
	_ io.ReaderAt              = (*opfsFile)(nil)
	_ io.WriterAt              = (*opfsFile)(nil)
	_ blob.Reader              = (*opfsFile)(nil)
	_ blob.ReaderAt            = (*opfsFile)(nil)
	_ blob.Writer              = (*opfsFile)(nil)
	_ blob.WriterAt            = (*opfsFile)(nil)
)

func (f *opfsFile) Stat() (hackpadfs.FileInfo, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, hackpadfs.ErrClosed
	}
	// Flush pending writes before stat, so getFile() sees committed data
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

func (f *opfsFile) Read(p []byte) (int, error) {
	return f.readAt(p, -1)
}

func (f *opfsFile) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(p, off)
}

func (f *opfsFile) ReadBlob(length int) (blob.Blob, int, error) {
	f.mu.Lock()
	off := f.offset
	f.mu.Unlock()
	b, n, err := f.readBlobAt(length, off)
	f.mu.Lock()
	f.offset += int64(n)
	f.mu.Unlock()
	return b, n, err
}

func (f *opfsFile) ReadBlobAt(length int, off int64) (blob.Blob, int, error) {
	b, n, err := f.readBlobAt(length, off)
	if n == 0 && err != nil {
		return b, 0, err
	}
	return b, n, err
}

func (f *opfsFile) readBlobAt(length int, off int64) (blob.Blob, int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return blob.NewBytes(nil), 0, hackpadfs.ErrClosed
	}
	if f.isDir {
		f.mu.Unlock()
		return blob.NewBytes(nil), 0, &hackpadfs.PathError{Op: "read", Path: f.name, Err: hackpadfs.ErrIsDir}
	}
	// If there's an active writer, flush it first so getFile() sees committed data.
	// This is critical for use cases like "write then stat/read" (e.g., Go module
	// download validates the zip after writing through the same file handle).
	if !f.writer.IsUndefined() {
		writer := f.writer
		f.writer = js.Undefined()
		f.file = js.Undefined()
		f.mu.Unlock()
		// close commits pending writes to OPFS
		_, err := awaitErr(writer.Call("close"))
		if err != nil {
			return blob.NewBytes(nil), 0, err
		}
		f.mu.Lock()
	}
	if f.file.IsUndefined() {
		file, err := awaitErr(f.handle.Call("getFile"))
		if err != nil {
			f.mu.Unlock()
			return blob.NewBytes(nil), 0, err
		}
		f.file = file
	}
	f.mu.Unlock()

	size := int64(f.file.Get("size").Float())
	if off >= size {
		return blob.NewBytes(nil), 0, io.EOF
	}

	end := off + int64(length)
	if end > size {
		end = size
	}

	blobVal := f.file.Call("slice", off, end)
	arrBuf, err := awaitErr(blobVal.Call("arrayBuffer"))
	if err != nil {
		return blob.NewBytes(nil), 0, err
	}

	jsBuf := js.Global().Get("Uint8Array").New(arrBuf)
	data := make([]byte, jsBuf.Length())
	n := js.CopyBytesToGo(data, jsBuf)

	return blob.NewBytes(data), n, nil
}

func (f *opfsFile) readAt(p []byte, off int64) (int, error) {
	useOffset := off < 0
	if useOffset {
		f.mu.Lock()
		off = f.offset
		f.mu.Unlock()
	}
	b, n, err := f.readBlobAt(len(p), off)
	if b != nil {
		copy(p, b.Bytes())
	}
	if useOffset {
		f.mu.Lock()
		f.offset += int64(n)
		f.mu.Unlock()
	}
	return n, err
}

func (f *opfsFile) WriteBlob(src blob.Blob) (int, error) {
	return f.Write(src.Bytes())
}

func (f *opfsFile) WriteBlobAt(src blob.Blob, off int64) (int, error) {
	return f.WriteAt(src.Bytes(), off)
}

func (f *opfsFile) WriteAt(p []byte, off int64) (int, error) {
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

func (f *opfsFile) Write(p []byte) (int, error) {
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
		// For append mode, always read the current file size first
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
		// Close existing writer to flush pending data, then re-create
		// so getFile() returns the correct (committed) size.
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

func (f *opfsFile) Close() error {
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

func (f *opfsFile) Seek(offset int64, whence int) (int64, error) {
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

func (f *opfsFile) Truncate(size int64) error {
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

func (f *opfsFile) Sync() error {
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

func (f *opfsFile) ReadDir(n int) ([]hackpadfs.DirEntry, error) {
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
		// Filter hidden metadata entries (prefixed with #)
		if strings.HasPrefix(entryName, "#") {
			res, err = awaitErr(iter.Call("next"))
			if err != nil {
				return nil, err
			}
			continue
		}
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

	return entries, nil
}

func (f *opfsFile) Chmod(mode hackpadfs.FileMode) error {
	f.fsys.meta.setMode(f.name, mode)
	return nil
}

func (f *opfsFile) Chown(uid, gid int) error {
	f.fsys.meta.setChown(f.name, uid, gid)
	return nil
}

func (f *opfsFile) Chtimes(atime, mtime time.Time) error {
	f.fsys.meta.setTimes(f.name, atime, mtime)
	return nil
}
