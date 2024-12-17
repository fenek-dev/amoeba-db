package main

import (
	"fmt"

	"github.com/fenek-dev/amoeba-db/utils"
)

const (
	PAGE_HEADERS_SIZE = 16
)

type Page struct {
	Index     uint64
	NextPage  uint64
	FreeSpace uint32
	End       uint32
	Data      []byte
}

func (conn *Connection) CreatePage(tableName string) (*Page, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	table, ok := conn.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", table)
	}

	page := Page{
		Index:     uint64(table.Headers.PageNum),
		NextPage:  0,
		FreeSpace: uint32(TABLE_PAGE_SIZE - PAGE_HEADERS_SIZE),
		End:       0,
		Data:      make([]byte, TABLE_PAGE_SIZE-PAGE_HEADERS_SIZE),
	}

	buf := make([]byte, TABLE_PAGE_SIZE)
	copy(buf[:8], utils.Uint64ToBytes(page.NextPage))
	copy(buf[8:12], utils.Uint32ToBytes(page.FreeSpace))
	copy(buf[12:16], utils.Uint32ToBytes(page.End))

	offset := CalculatePageAddress(int64(table.Headers.PageNum))

	_, err := table.fd.WriteAt(buf, offset)
	if err != nil {
		return nil, err
	}

	table.Headers.PageNum++

	return &page, nil
}

func (p *Page) WriteLine(line []byte) error {
	if len(line) > int(p.FreeSpace) {
		return fmt.Errorf("line too long")
	}

	copy(p.Data[p.End:], line)
	p.End += uint32(len(line))
	p.FreeSpace -= uint32(len(line))

	return nil
}

func (p *Page) ReadRow(index int64, rowSize int64) ([]byte, error) {
	offset := uint32(index * rowSize)
	if offset >= p.End {
		return nil, fmt.Errorf("invalid offset")
	}

	return p.Data[offset : int64(offset)+rowSize], nil
}
