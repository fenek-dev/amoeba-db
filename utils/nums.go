package utils

import "encoding/binary"

func Uint64ToBytes(num uint64) []byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], num)
	return b[:]
}

func Uint32ToBytes(num uint32) []byte {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], num)
	return b[:]
}

func Uint16ToBytes(num uint16) []byte {
	b := [2]byte{}
	binary.BigEndian.PutUint16(b[:], num)
	return b[:]
}
