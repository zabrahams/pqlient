package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
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

	TRANS_STATUS_IDLE     = 'I'
	TRANS_STATUS_IN_TRANS = 'T'
	TRANS_STATUS_FAILED   = 'E'
)

type Conn struct {
	conn              net.Conn
	reader            *bufio.Reader
	params            map[string]string
	pid               int32
	secretKey         int32
	transactionStatus byte
}

func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn:   conn,
		params: make(map[string]string),
		reader: bufio.NewReader(conn),
	}
}

func (c *Conn) sendMsg(msg *bytes.Buffer) error {
	writer := bufio.NewWriter(c.conn)
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

func (c *Conn) close() {
	c.conn.Close()
}

func (c *Conn) readMsg() (byte, []byte, error) {
	fmt.Println("reading")
	respType, err := c.reader.ReadByte()
	if err != nil {
		log.Fatal(err)
	}

	sizeInBytes := make([]byte, 4)
	_, err = io.ReadFull(c.reader, sizeInBytes)
	if err != nil {
		log.Fatal(err)
	}
	var size int32
	err = binary.Read(bytes.NewBuffer(sizeInBytes), binary.BigEndian, &size)
	if err != nil {
		log.Fatal(err)
	}

	resp := make([]byte, size-4)
	_, err = io.ReadFull(c.reader, resp)
	if err != nil {
		log.Fatal(err)
	}

	return respType, resp, nil
}

func (c *Conn) handleResp(respType byte, resp []byte) {
	switch respType {
	case byte('C'):
		fmt.Println("parsing close command")
		err := c.handleCloseResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	case byte('E'):
		fmt.Println("parsing error response")
		err := c.handleErrorResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	case byte('K'):
		fmt.Println("parsing backend key data")
		err := c.handleBackendKeyDataResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	case byte('R'):
		fmt.Println("parsing auth response")
		err := c.handleAuthResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	case byte('S'):
		fmt.Println("parsing parameter response")
		err := c.handleParameterStatusResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	case byte('Z'):
		fmt.Println("parsing ready for query response")
		err := c.handleReadyForQueryResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
	default:
		spew.Dump(c)
		log.Fatalf("unknown response type: %c", respType)
	}
}

func (c *Conn) handleAuthResponse(resp []byte) error {
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

func (c *Conn) handleCloseResponse(resp []byte) error {
	b := bytes.NewBuffer(resp)
	statementType, err := b.ReadByte()
	if err != nil {
		return err
	}

	if statementType == 'S' {
		fmt.Println("closing prepared statement")
	} else if statementType == 'P' {
		fmt.Println("closing portal")
	} else {
		return errors.New("unknown statement type closed")
	}

	name, err := readString(b)
	if err != nil {
		return err
	}

	if name != "" {
		fmt.Printf("statement is named: %s\n", name)
	}

	return nil
}

func (c *Conn) handleDataRowResponse(resp []byte) error {
	b := bytes.NewBuffer(resp)
	columns, err := readColumns(b)
	if err != nil {
		return err
	}

	spew.Dump(columns)
	return nil
}

func (c *Conn) handleErrorResponse(resp []byte) error {
	errors := make(map[byte]string)
	buff := bytes.NewBuffer(resp)
	for {
		b, err := buff.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if b == 0 {
			break
		}
		k := b
		v, err := readString(buff)
		if err != nil {
			return err
		}
		errors[k] = v
	}

	for k, v := range errors {
		fmt.Printf("field: %c\nvalue: %s\n", k, v)
	}
	log.Fatalf("error response")
	return nil
}

func (c *Conn) handleParameterStatusResponse(resp []byte) error {
	paramSlice := bytes.Split(resp, []byte{0})
	k, v := string(paramSlice[0]), string(paramSlice[1])
	c.params[k] = v
	fmt.Println("new param received")
	return nil
}

func (c *Conn) handleBackendKeyDataResponse(resp []byte) error {
	var pid, secretKey int32
	pidReader := bytes.NewBuffer(resp[:4])
	skeyReader := bytes.NewBuffer(resp[4:8])
	binary.Read(pidReader, binary.BigEndian, &pid)
	binary.Read(skeyReader, binary.BigEndian, &secretKey)
	c.secretKey = secretKey
	c.pid = pid
	fmt.Println("Received BACKEND KEY DATA")
	return nil
}

func (c *Conn) handleReadyForQueryResponse(resp []byte) error {
	c.transactionStatus = resp[0]
	fmt.Printf("Ready For Query - transaction status: %c\n", resp[0])
	return nil
}
