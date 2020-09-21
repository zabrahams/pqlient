package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/davecgh/go-spew/spew"
)

func main() {

	rawConn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		log.Fatal(err)
	}
	conn := newConn(rawConn)
	defer conn.close()

	stMsg, err := startupMsg()
	if err != nil {
		log.Fatal(err)
	}

	err = conn.sendMsg(stMsg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn.readAndHandle()
		if conn.transactionStatus != 0 {
			break
		}
	}

	queryMsg, err := testQuery()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("sending test query")
	err = conn.sendMsg(queryMsg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn.readAndHandle()
	}

	tMsg, err := terminateMsg()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("send terminate to close connection")
	err = conn.sendMsg(tMsg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("connection terminated and session complete")
}

func startupMsg() (*bytes.Buffer, error) {
	buff := new(bytes.Buffer)
	// buffer for the startup message
	err := binary.Write(buff, binary.BigEndian, int32(196608)) // protocol version
	if err != nil {
		return nil, err
	}

	buff.WriteString("user")
	buff.WriteByte(0)

	buff.WriteString("zee")
	buff.WriteByte(0)

	buff.WriteByte(0)

	len := buff.Len() + 4

	msg := new(bytes.Buffer)
	err = binary.Write(msg, binary.BigEndian, int32(len))
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(msg, buff)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func terminateMsg() (*bytes.Buffer, error) {
	buff := new(bytes.Buffer)
	buff.WriteByte('X')
	binary.Write(buff, binary.BigEndian, int32(4))
	buff.WriteByte(0)
	return buff, nil
}

func testQuery() (*bytes.Buffer, error) {
	query := "SELECT * FROM foo"
	buff := new(bytes.Buffer)
	buff.WriteByte('Q')
	err := binary.Write(buff, binary.BigEndian, int32(len(query)+5))
	if err != nil {
		log.Fatal(err)
	}
	buff.WriteString(query)
	buff.WriteByte(0)

	spew.Dump(buff.Bytes())
	return buff, nil
}
