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

const (
	AUTHENTICATION_OK                 = 0
	AUTHENTICATION_KERBEROS_V5        = 2
	AUTHENTICATION_CLEARTEXT_PASSWORD = 3
	AUTHENTICATION_MD5_PASSWORD       = 5
	// there are lots more but I dont see them coming up.
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
		case byte('R'):
			fmt.Println("parsing auth response")
			err = parseAuthResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		case byte('S'):
			fmt.Println("parsing parameter response")
			err = parseParameterStatusResponse(resp)
			if err != nil {
				log.Fatal(err)
			}
		default:
			log.Fatalf("unknown response type: %c", respType)
		}
	}

}

func parseAuthResponse(resp []byte) error {
	var authRespType int32
	reader := bytes.NewBuffer(resp[:3])
	binary.Read(reader, binary.BigEndian, &authRespType)
	switch authRespType {
	case AUTHENTICATION_OK:
		fmt.Println("received AUTHENTICATION OK")
	case AUTHENTICATION_CLEARTEXT_PASSWORD:
		fmt.Println("received AUTHENTICATION_CLEARTEXT_PASSWORD")
	case AUTHENTICATION_KERBEROS_V5:
		fmt.Println("received AUTHENTICATION_KERBEROS_V5")
	case AUTHENTICATION_MD5_PASSWORD:
		fmt.Println("received AUTHENTICATION_MD5_PASSWORD")
	}

	return nil
}

func parseParameterStatusResponse(resp []byte) error {
	paramSlice := bytes.Split(resp, []byte{0})
	fmt.Printf("PARAM: %s\nVALUE: %s\n\n", string(paramSlice[0]), string(paramSlice[1]))
	return nil
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
