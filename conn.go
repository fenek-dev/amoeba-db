package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const (
	MAGIC_STRING = "AMOEBAFILE"
)

type Headers struct {
	MagicString [10]byte
	TableCount  uint32
}

type Connection struct {
	path string
	flag int

	fd      *os.File
	headers Headers

	tables map[string]Table

	mu sync.RWMutex
}

func Connect(path, name string, flag int) *Connection {
	fd, err := os.OpenFile(path+"/"+name, flag|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	conn := &Connection{
		fd:   fd,
		path: path,
		flag: flag,

		tables: make(map[string]Table),
	}

	err = conn.ParseHeaders(path + "/" + name)
	if err != nil {
		panic(err)
	}

	return conn
}

func (conn *Connection) Close() error {
	return conn.fd.Close()
}

func (conn *Connection) ParseHeaders(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	copy(conn.headers.MagicString[:], content[:10])
	if len(content) < 10 {
		return conn.WriteHeaders()
	}

	return conn.ReadHeaders(content)
}

func (conn *Connection) ReadHeaders(content []byte) error {

	copy(conn.headers.MagicString[:], content[:10])

	if string(conn.headers.MagicString[:]) != MAGIC_STRING {
		return fmt.Errorf("invalid magic string")
	}

	conn.headers.TableCount = binary.BigEndian.Uint32(content[10:14])
	return nil
}

func (conn *Connection) WriteHeaders() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	_, err := conn.fd.Write([]byte(MAGIC_STRING))
	if err != nil {
		return err
	}

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, conn.headers.TableCount)
	_, err = conn.fd.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
