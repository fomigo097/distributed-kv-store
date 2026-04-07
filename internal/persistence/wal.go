package persistence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// CommandSet stores or overwrites a value.
	CommandSet byte = 1
	// CommandGet reads a value.
	CommandGet byte = 2
	// CommandDelete removes a key.
	CommandDelete byte = 3
)

var errUnsupportedCommand = errors.New("unsupported command")

// Record is a single protocol and WAL entry.
type Record struct {
	Command byte
	Key     string
	Value   string
}

// WAL appends mutating commands and can replay them on restart.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// OpenWAL opens or creates the log file at path.
func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file}, nil
}

// Append persists a mutating record before the write is acknowledged.
func (w *WAL) Append(record Record) error {
	if record.Command != CommandSet && record.Command != CommandDelete {
		return errUnsupportedCommand
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := writeRecord(w.file, record); err != nil {
		return err
	}

	return w.file.Sync()
}

// Replay streams every persisted record from the beginning of the file.
func (w *WAL) Replay(apply func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	for {
		record, err := readRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := apply(record); err != nil {
			return err
		}
	}

	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// WriteRequest writes a protocol record to a writer.
func WriteRequest(writer io.Writer, record Record) error {
	return writeRecord(writer, record)
}

// ReadRequest reads a protocol record from a reader.
func ReadRequest(reader io.Reader) (Record, error) {
	return readRecord(reader)
}

func writeRecord(writer io.Writer, record Record) error {
	if err := writeByte(writer, record.Command); err != nil {
		return err
	}
	if err := writeBytes(writer, []byte(record.Key)); err != nil {
		return err
	}
	return writeBytes(writer, []byte(record.Value))
}

func readRecord(reader io.Reader) (Record, error) {
	cmd, err := readByte(reader)
	if err != nil {
		return Record{}, err
	}

	key, err := readBytes(reader)
	if err != nil {
		return Record{}, err
	}

	value, err := readBytes(reader)
	if err != nil {
		return Record{}, err
	}

	return Record{
		Command: cmd,
		Key:     string(key),
		Value:   string(value),
	}, nil
}

func writeByte(writer io.Writer, value byte) error {
	_, err := writer.Write([]byte{value})
	return err
}

func readByte(reader io.Reader) (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(reader, buf[:])
	return buf[0], err
}

func writeBytes(writer io.Writer, value []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))

	if _, err := writer.Write(lenBuf[:]); err != nil {
		return err
	}

	if len(value) == 0 {
		return nil
	}

	_, err := writer.Write(value)
	return err
}

func readBytes(reader io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(lenBuf[:])
	if size == 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
