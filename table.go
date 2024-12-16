package main

import (
	"fmt"
	"os"

	"github.com/fenek-dev/amoeba-db/utils"
)

type Column struct {
	Name string // max 32 bytes
	Type uint16
	Size uint16
}

type TableHeaders struct {
	Name      [32]byte
	PageNum   uint32
	ColumnNum uint16
	Columns   []Column
}

type Page struct {
	NextPage  uint64
	PrevPage  uint64
	RowCount  uint32
	FreeSpace uint32
}

type Table struct {
	fd      *os.File
	Headers TableHeaders
}

func (conn *Connection) CreateTable(name string, columns ...Column) error {
	fd, err := os.OpenFile(conn.path+"/"+name+".table", conn.flag|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	if len([]byte(name)) > 64 {
		return fmt.Errorf("name is too long")
	}

	if len(columns) > 32 {
		return fmt.Errorf("too many columns")
	}

	for _, col := range columns {
		if len([]byte(col.Name)) > 32 {
			return fmt.Errorf("column name is too long")
		}
	}

	nameBuf := [32]byte{}
	copy(nameBuf[:], []byte(name))

	table := Table{
		fd: fd,
		Headers: TableHeaders{
			Name:      nameBuf,
			PageNum:   1,
			ColumnNum: uint16(len(columns)),
			Columns:   columns,
		},
	}

	// |------------------------HEADERS----------------------------|
	// | table name 32B | page num 4B | col num 4B |               |
	// | col1 name 32B | col1 type 2B | col1 size 2B |             |
	// |-----------------------------------------------------------|
	// 32B + 4B + 4B + ( 32B + 2B + 2B ) * 32 = 1188
	//
	// Reserve 1188 bytes for headers and 32 columns

	buf := make([]byte, 1188)
	copy(buf[:32], table.Headers.Name[:])
	copy(buf[32:36], utils.Uint32ToBytes(table.Headers.PageNum))
	copy(buf[36:38], utils.Uint16ToBytes(table.Headers.ColumnNum))

	for i, col := range table.Headers.Columns {
		offset := 38 + i*36
		copy(buf[offset:offset+32], col.Name[:])
		copy(buf[offset+32:offset+34], utils.Uint16ToBytes(col.Type))
		copy(buf[offset+34:offset+36], utils.Uint16ToBytes(col.Size))
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	_, err = fd.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
