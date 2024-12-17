package main

const (
	TABLE_PAGE_OFFSET = 1186

	TABLE_PAGE_SIZE = 32_768
)

func CalculatePageAddress(index int64) int64 {
	return TABLE_PAGE_OFFSET + (index * TABLE_PAGE_SIZE)
}
