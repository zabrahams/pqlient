package main

import (
	"bytes"
	"encoding/binary"
)

const (
	FORMAT_CODE_BINARY = 1
	FORMAT_CODE_TEXT   = 0
)

type field struct {
	name          string
	tableOID      int32
	columnAttrNum int16
	typeOID       int32
	typeSize      int16
	typeMod       int32
	formatCode    int16
}

func readField(b *bytes.Buffer) (field, error) {
	var name string
	var tableOID, typeMod, typeOID int32
	var columnAttrNum, typeSize, formatCode int16
	name, err := readString(b)
	if err != nil {
		return field{}, err
	}
	err = binary.Read(b, binary.BigEndian, &tableOID)
	if err != nil {
		return field{}, err
	}

	err = binary.Read(b, binary.BigEndian, &columnAttrNum)
	if err != nil {
		return field{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeOID)
	if err != nil {
		return field{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeSize)
	if err != nil {
		return field{}, err
	}

	err = binary.Read(b, binary.BigEndian, &typeMod)
	if err != nil {
		return field{}, err
	}

	err = binary.Read(b, binary.BigEndian, &formatCode)
	if err != nil {
		return field{}, err
	}

	f := field{
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

func readFields(b *bytes.Buffer) ([]field, error) {
	var numFields int16
	err := binary.Read(b, binary.BigEndian, &numFields)
	if err != nil {
		return nil, err
	}

	fields := make([]field, numFields)
	for i := 0; i < int(numFields); i++ {
		f, err := readField(b)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}

	return fields, nil
}
