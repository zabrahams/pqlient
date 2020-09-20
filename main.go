package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {

	conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stMsg, err := startupMsg()
	if err != nil {
		log.Fatal(err)
	}

	err = sendMsg(conn, stMsg)
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(conn)
	resp := make([]byte, 1000)
	for {
		size, err := reader.ReadByte()
		if err != nil {
			log.Fatal(err)
		}

		_, err = io.ReadFull(reader, resp[:int(size)])
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("received %x\n", resp[:int(size)])
	}

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

func sendMsg(conn net.Conn, msg *bytes.Buffer) error {
	writer := bufio.NewWriter(conn)
	written, err := writer.Write(msg.Bytes())
	if err != nil {
		return err
	}
	fmt.Printf("message sent: %d bytes\n", written)
	err = writer.Flush()
	if err != nil {
		return err
	}

	return nil
}
