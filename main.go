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

	conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	buff := new(bytes.Buffer)
	spew.Dump(buff.String())
	// buffer for the startup message
	err = binary.Write(buff, binary.BigEndian, int32(196608)) // protocol version
	if err != nil {
		log.Fatal(err)
	}

	buff.WriteString("user")
	// buff.WriteByte(0)

	buff.WriteString("zee")
	buff.WriteByte(0)

	buff.WriteByte(0)

	length := buff.Len() + 4
	fmt.Println("writing to conn")
	err = binary.Write(conn, binary.BigEndian, int32(length)) // protocol version
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("length written")
	written, err := io.Copy(conn, buff)

	fmt.Printf("startup message sent: %d bytes", written)

}
