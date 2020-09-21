package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/davecgh/go-spew/spew"
)

type Conn struct {
	conn      net.Conn
	params    map[string]string
	pid       int32
	secretKey int32
}

func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn:   conn,
		params: make(map[string]string),
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

func (c *Conn) newReader() *bufio.Reader {
	return bufio.NewReader(c.conn)
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

func (c *Conn) handleParameterStatusResponse(resp []byte) error {
	paramSlice := bytes.Split(resp, []byte{0})
	k, v := string(paramSlice[0]), string(paramSlice[1])
	c.params[k] = v
	fmt.Println("new param received")
	return nil
}

func (c *Conn) handleBackendKeyDataResponse(resp []byte) error {
	spew.Dump(resp)
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
