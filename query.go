package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type SimpleQuery struct {
	columnDescriptions []colDesc
	rows               [][][]byte
	query              string
	conn               *Conn
}

func NewSimpleQuery(conn *Conn, query string) *SimpleQuery {
	return &SimpleQuery{
		conn:  conn,
		query: query,
		rows:  make([][][]byte, 0),
	}
}

func (q *SimpleQuery) Exec() error {
	encodedQuery, err := q.encodeQuery()
	if err != nil {
		return err
	}

	err = q.conn.sendMsg(encodedQuery)
	if err != nil {
		return err
	}

	for {
		msgType, resp, err := q.conn.readMsg()
		switch msgType {
		case byte('C'):
			fmt.Println("closing query")
			err = q.conn.handleCloseResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
			break
		case byte('D'):
			fmt.Println("parsing data row response")
			err = q.handleDataRowResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		case byte('T'):
			fmt.Println("parsing row description response")
			err = q.handleRowDescriptionResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	return nil
}

func (q *SimpleQuery) encodeQuery() (*bytes.Buffer, error) {
	buff := new(bytes.Buffer)
	buff.WriteByte('Q')
	err := binary.Write(buff, binary.BigEndian, int32(len(q.query)+5))
	if err != nil {
		log.Fatal(err)
	}
	buff.WriteString(q.query)
	buff.WriteByte(0)

	return buff, nil
}

func (q *SimpleQuery) handleRowDescriptionResponse(resp []byte) error {
	fmt.Println("got row description!")
	colDescs, err := readColDescs(bytes.NewBuffer(resp))
	if err != nil {
		return err
	}
	q.columnDescriptions = colDescs
	return nil
}

func (q *SimpleQuery) handleDataRowResponse(resp []byte) error {
	b := bytes.NewBuffer(resp)
	row, err := readColumns(b)
	if err != nil {
		return err
	}

	q.rows = append(q.rows, row)
	return nil
}
