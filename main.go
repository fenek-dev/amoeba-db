package main

import (
	"fmt"
)

func main() {

	c := Connect(".data", "test.db", READWRITE)
	defer c.Close()

	t, err := c.OpenTable(
		"users",
	)

	//t, err := c.CreateTable(
	//	"users",
	//	Column{
	//		Name: "id",
	//		Type: T_SERIAL,
	//		Size: 8,
	//	},
	//	Column{
	//		Name: "name",
	//		Type: T_STRING,
	//		Size: 32,
	//	},
	//)

	if err != nil {
		panic(err)
	}

	//p, err := c.CreatePage("users")
	p, err := t.ReadPage(0)
	if err != nil {
		panic(err)
	}

	fmt.Println(t.lineSize)

	b, _ := p.ReadRow(0, int64(t.lineSize))

	fmt.Println(b)

	//line, err := t.CreateLine(ColumnValue{
	//	Name:  "id",
	//	Value: []byte("1"),
	//}, ColumnValue{
	//	Name:  "name",
	//	Value: []byte("John"),
	//})
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = p.WriteLine(line)
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = t.WritePage(p)
	//if err != nil {
	//	panic(err)
	//}

	//p2, err := t.ReadPage(0)
	//if err != nil {
	//	panic(err)
	//}
	//
	//p2.ReadRow()

	//fmt.Println(p2.Data)

}
