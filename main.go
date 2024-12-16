package main

func main() {

	c := Connect(".data", "test.db", READWRITE)

	err := c.CreateTable("users", Column{
		Name: "id",
		Type: T_INT,
		Size: 4,
	})

	if err != nil {
		panic(err)
	}

}
