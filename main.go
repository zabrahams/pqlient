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
		msgType, resp, err := conn.readMsg()
		if err != nil {
			log.Fatal(err)
		}

		conn.handleResp(msgType, resp)
		if conn.transactionStatus != 0 {
			break
		}
	}

	query := NewSimpleQuery(conn, "SELECT * FROM foo;")

	err = query.Exec()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Query handled")
	spew.Dump("Results:", query.rows)

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

func readString(b *bytes.Buffer) (string, error) {
	bs := []byte{}
	for {
		next, err := b.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		} else if next == 0 {
			break
		}

		bs = append(bs, next)
	}

	return string(bs), nil
}
