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

type TableLine struct {
	ID   [16]byte
	Name [32]byte
}

type Connection struct {
	path string
	flag int

	fd      *os.File
	headers Headers
	tables  []TableLine

	tableFDs map[string]*os.File

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

	for i := 0; i < int(conn.headers.TableCount); i++ {
		var table TableLine
		copy(table.ID[:], content[14+i*80:30+i*80])
		copy(table.Name[:], content[30+i*80:94+i*80])
		conn.tables = append(conn.tables, table)
	}

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

	buf = make([]byte, len(conn.tables)*80)
	for i, table := range conn.tables {
		copy(buf[i*80:], table.ID[:])
		copy(buf[i*80+16:], table.Name[:])
	}

	_, err = conn.fd.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
