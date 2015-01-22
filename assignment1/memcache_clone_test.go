package main

import (
	"testing"
	"net"
	"strings"
)

func TestMain(t *testing.T) {
	go main()
}

func TestCase(t *testing.T) {

	cases:= []struct {
		in, want string
	} {
		{"set dushyant 200 10 gulf-talent","OK 1001"},
		{"set ravi 1 11 yodlee-tech","OK 1002"},
		{"set rahul 1 9 db-phatak","OK 1003"},
		{"delete raavi","ERRNOTFOUND"},
		{"delete ravi","ERRNOTFOUND"},
		{"cas dushyant 300 1001 4 MSCI","OK 1004"},
	}

	for _, c := range cases {

		tcpAddr, err := net.ResolveTCPAddr(CONN_TYPE,CONN_HOST+ ":" +CONN_PORT)
		conn, err := net.DialTCP("tcp", nil, tcpAddr)

		if(err!=nil) {
			t.Errorf("Error Dialing to the server")	
		}
	
		defer conn.Close()
	
		conn.Write([]byte(c.in))
		
		got:=make([]byte,1024)

		size,err:=conn.Read(got)

		if(err!=nil) {
			t.Errorf("Error Reading from the server",err.Error())
		}

		got=got[:size]

		response:= string(got)
		response=strings.TrimSpace(response)

		if(c.want!=response) {
			t.Errorf("Expected: %s Got:%s",c.want,string(got))
		}
	}

}