package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/fenek-dev/amoeba-db/utils"
)

type Column struct {
	Name string // max 32 bytes
	Type uint16
	Size uint16
}

type ColumnValue struct {
	Name  string // max 32 bytes
	Value []byte
}

type TableHeaders struct {
	Name      [32]byte
	PageNum   uint32
	ColumnNum uint16
	Columns   []Column
}

type Table struct {
	fd      *os.File
	Headers TableHeaders

	lineSize uint64
}

func (conn *Connection) CreateTable(name string, columns ...Column) (*Table, error) {

	path := conn.path + "/" + name + ".table"

	fmt.Println(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return nil, fmt.Errorf("table %s already exists", name)
	}

	fd, err := os.OpenFile(conn.path+"/"+name+".table", conn.flag|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	if len([]byte(name)) > 64 {
		return nil, fmt.Errorf("name is too long")
	}

	if len(columns) > 32 {
		return nil, fmt.Errorf("too many columns")
	}

	for _, col := range columns {
		if len([]byte(col.Name)) > 32 {
			return nil, fmt.Errorf("column name is too long")
		}
	}

	nameBuf := [32]byte{}
	copy(nameBuf[:], []byte(name))

	table := Table{
		fd: fd,
		Headers: TableHeaders{
			Name:      nameBuf,
			PageNum:   0,
			ColumnNum: uint16(len(columns)),
			Columns:   columns,
		},
		lineSize: countLineSize(columns...),
	}

	buf := table.HeadersBuf()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	_, err = fd.Write(buf)
	if err != nil {
		return nil, err
	}

	conn.tables[name] = table

	return &table, nil
}

func (conn *Connection) OpenTable(name string) (*Table, error) {

	path := conn.path + "/" + name + ".table"

	fd, err := os.OpenFile(path, conn.flag, 0666)
	if err != nil {
		return nil, err
	}

	headers, err := conn.ReadTableHeaders(fd)
	if err != nil {
		return nil, err
	}

	table := Table{
		fd:       fd,
		Headers:  headers,
		lineSize: countLineSize(headers.Columns...),
	}

	conn.tables[name] = table

	return &table, nil
}

func (conn *Connection) ReadTableHeaders(fd *os.File) (TableHeaders, error) {

	buf := make([]byte, 1186)

	_, err := fd.Read(buf)
	if err != nil {
		return TableHeaders{}, err
	}

	headers := TableHeaders{
		Name:      [32]byte{},
		PageNum:   binary.BigEndian.Uint32(buf[32:36]),
		ColumnNum: binary.BigEndian.Uint16(buf[36:38]),
	}

	copy(headers.Name[:], buf[:32])

	for i := 0; i < int(headers.ColumnNum); i++ {
		offset := 38 + i*36
		col := Column{
			Name: strings.Trim(string(buf[offset:offset+32]), "\x00"),
			Type: binary.BigEndian.Uint16(buf[offset+32 : offset+34]),
			Size: binary.BigEndian.Uint16(buf[offset+34 : offset+36]),
		}

		headers.Columns = append(headers.Columns, col)
	}

	return headers, nil

}

func (t *Table) WriteHeaders() error {
	_, err := t.fd.Seek(0, 0)
	if err != nil {
		return err
	}

	buf := t.HeadersBuf()

	_, err = t.fd.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) WritePage(page *Page) error {
	buf := make([]byte, TABLE_PAGE_SIZE)

	copy(buf[:8], utils.Uint64ToBytes(page.NextPage))
	copy(buf[8:12], utils.Uint32ToBytes(page.FreeSpace))
	copy(buf[12:16], utils.Uint32ToBytes(page.End))
	copy(buf[16:], page.Data)

	_, err := t.fd.WriteAt(buf, CalculatePageAddress(int64(page.Index)))
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) HeadersBuf() []byte {

	// |------------------------HEADERS----------------------------|
	// | table name 32B | page num 4B | col num 2B |               |
	// | col1 name 32B | col1 type 2B | col1 size 2B |             |
	// |-----------------------------------------------------------|
	// 32B + 4B + 2B + ( 32B + 2B + 2B ) * 32 = 1186
	//
	// Reserve 1186 bytes for headers and 32 columns

	buf := make([]byte, 1186)

	copy(buf[:32], t.Headers.Name[:])
	copy(buf[32:36], utils.Uint32ToBytes(t.Headers.PageNum))
	copy(buf[36:38], utils.Uint16ToBytes(t.Headers.ColumnNum))

	for i, col := range t.Headers.Columns {
		offset := 38 + i*36
		copy(buf[offset:offset+32], col.Name[:])
		copy(buf[offset+32:offset+34], utils.Uint16ToBytes(col.Type))
		copy(buf[offset+34:offset+36], utils.Uint16ToBytes(col.Size))
	}

	return buf
}

func (t *Table) ReadPage(index int64) (*Page, error) {
	buf := make([]byte, TABLE_PAGE_SIZE)

	red, err := t.fd.ReadAt(buf, CalculatePageAddress(index))

	if err != nil {
		return nil, err
	}

	if red != TABLE_PAGE_SIZE {
		return nil, fmt.Errorf("invalid page size")
	}

	page := Page{
		Index:     uint64(index),
		NextPage:  binary.BigEndian.Uint64(buf[:8]),
		FreeSpace: binary.BigEndian.Uint32(buf[8:12]),
		End:       binary.BigEndian.Uint32(buf[12:16]),
		Data:      buf[16:],
	}

	return &page, nil
}

func (t *Table) CreateLine(args ...ColumnValue) ([]byte, error) {

	if len(args) != len(t.Headers.Columns) {
		return nil, fmt.Errorf("invalid number of arguments")
	}

	buf := make([]byte, t.lineSize)

	offset := 0

	for i := 0; i < len(args); i++ {
		col := t.Headers.Columns[i]
		arg := args[i]

		if len(arg.Name) > 32 {
			return nil, fmt.Errorf("column name is too long")
		}

		if len(arg.Value) > int(col.Size) {
			return nil, fmt.Errorf("value is too long")
		}

		if arg.Name != col.Name {
			return nil, fmt.Errorf("invalid columns sequence or name")
		}

		copy(buf[offset:offset+int(col.Size)], arg.Value)
		offset += int(col.Size)
	}

	return buf, nil

}

func countLineSize(columns ...Column) uint64 {
	var size uint64

	for _, col := range columns {
		size += uint64(col.Size)
	}

	return size
}
