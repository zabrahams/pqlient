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

const (
	AUTHENTICATION_OK                 = 0
	AUTHENTICATION_KERBEROS_V5        = 2
	AUTHENTICATION_CLEARTEXT_PASSWORD = 3
	AUTHENTICATION_MD5_PASSWORD       = 5
	// there are lots more but I dont see them coming up.
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

	reader := conn.newReader()
	for {
		fmt.Println("reading")
		respType, err := reader.ReadByte()
		if err != nil {
			log.Fatal(err)
		}

		sizeInBytes := make([]byte, 4)
		_, err = io.ReadFull(reader, sizeInBytes)
		if err != nil {
			log.Fatal(err)
		}
		var size int32
		err = binary.Read(bytes.NewBuffer(sizeInBytes), binary.BigEndian, &size)
		if err != nil {
			log.Fatal(err)
		}

		resp := make([]byte, size-4)
		_, err = io.ReadFull(reader, resp)
		if err != nil {
			log.Fatal(err)
		}

		switch respType {
		case byte('K'):
			fmt.Println("parsing backend key data")
			err = conn.handleBackendKeyDataResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		case byte('R'):
			fmt.Println("parsing auth response")
			err = conn.handleAuthResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		case byte('S'):
			fmt.Println("parsing parameter response")
			err = conn.handleParameterStatusResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		default:
			spew.Dump(conn)
			log.Fatalf("unknown response type: %c", respType)
		}
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
