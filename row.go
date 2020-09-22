package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

// This needs to handle both formats...
func readColumn(b *bytes.Buffer) ([]byte, error) {
	var length int32
	err := binary.Read(b, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	} else if length == -1 { //this means the value is NULL
		return nil, nil
	}

	data := make([]byte, length)
	_, err = io.ReadFull(b, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func readColumns(b *bytes.Buffer) ([][]byte, error) {
	var numCol int16
	err := binary.Read(b, binary.BigEndian, &numCol)
	if err != nil {
		return nil, err
	}
	columns := make([][]byte, numCol)
	for i := 0; i < int(numCol); i++ {
		col, err := readColumn(b)
		if err != nil {
			return nil, err
		}

		columns[i] = col
	}

	return columns, nil
}
