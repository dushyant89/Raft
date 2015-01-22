package main

import (
	"testing"
	"net"
	"strings"
	"time"
)

func TestMain(t *testing.T) {
	go main()
}

func TestCase(t *testing.T) {

	cases:= []struct {
		in, want string
		noReply bool
	} {
		{"set dushyant 200 10 gulf-talent","OK 1001",false},
		{"set ravi 1 11 yodlee-tech","OK 1002",false},
		{"set rahul 100 9 noreply db-phatak","",true},
		{"delete raavi","ERRNOTFOUND",false},
		{"delete ravi","ERRNOTFOUND",false},
		{"cas dushyant 300 1001 4 MSCI","OK 1004",false},
		{"getm rahul","VALUE 1003 100 9 db-phatak",false},
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
		  if(!c.noReply) {		
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

			time.Sleep(1*time.Second)
		}	
	}

}