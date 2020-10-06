package main

import (
	"bytes"
	"encoding/binary"
)

const (
	FORMAT_CODE_BINARY = 1
	FORMAT_CODE_TEXT   = 0
)

type colDesc struct {
	name          string
	tableOID      int32
	columnAttrNum int16
	typeOID       int32
	typeSize      int16
	typeMod       int32
	formatCode    int16
}

func readColDesc(b *bytes.Buffer) (colDesc, error) {
	var name string
	var tableOID, typeMod, typeOID int32
	var columnAttrNum, typeSize, formatCode int16
	name, err := readString(b)
	if err != nil {
		return colDesc{}, err
	}
	err = binary.Read(b, binary.BigEndian, &tableOID)
	if err != nil {
		return colDesc{}, err
	}

	err = binary.Read(b, binary.BigEndian, &columnAttrNum)
	if err != nil {
		return colDesc{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeOID)
	if err != nil {
		return colDesc{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeSize)
	if err != nil {
		return colDesc{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeMod)
	if err != nil {
		return colDesc{}, err
	}

	err = binary.Read(b, binary.BigEndian, &formatCode)
	if err != nil {
		return colDesc{}, err
	}

	f := colDesc{
		name:          name,
		tableOID:      tableOID,
		columnAttrNum: columnAttrNum,
		typeOID:       typeOID,
		typeSize:      typeSize,
		typeMod:       typeMod,
		formatCode:    formatCode,
	}

	return f, nil

}

func readColDescs(b *bytes.Buffer) ([]colDesc, error) {
	var numColDesc int16
	err := binary.Read(b, binary.BigEndian, &numColDesc)
	if err != nil {
		return nil, err
	}

	colDesc := make([]colDesc, numColDesc)
	for i := 0; i < int(numColDesc); i++ {
		f, err := readColDesc(b)
		if err != nil {
			return nil, err
		}
		colDesc[i] = f
	}

	return colDesc, nil
}
