package main

import (
	"testing"
	"net"
	//"strings"
	//"time"
	//"fmt"
)

func TestMain(t *testing.T) {

	//this call starts the server for listening to connections
	//go main()
}

type TestCase struct {
	in string
	want string
	noReply bool
}

var wait_ch chan int

func fireTestCases(t *testing.T, n int, testcases []TestCase) {
	
	wait_ch = make(chan int, n)
	
	for i := 0; i<n; i++ {
		go shootTestCase(t, i+1, testcases)
	}
	
	ended := 0
	
	for ended < n {
		<-wait_ch
		ended++
	}
}

func shootTestCase(t *testing.T, routineID int, testcases []TestCase) {
	
	//fmt.Println("Shooting test case for:",routineID)
	
	tcpAddr, err := net.ResolveTCPAddr(CONN_TYPE,CONN_HOST+ ":" +CONN_PORT)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	if(err!=nil) {
		t.Errorf("Error Dialing to the server")
	}
	
	defer conn.Close()

	for _, c := range testcases {
		
		conn.Write([]byte(c.in))

		got:=make([]byte,1024)
	    
	    if(!c.noReply) {		
		
			size,err:=conn.Read(got)

			if(err!=nil) {
				t.Errorf("Error Reading from the server",err.Error())
			}

			got=got[:size]
			response:= string(got)
			//response=strings.TrimSpace(response)

			if(c.want!=response) {
				t.Errorf("Expected: %s Got:%s for routine: %d",c.want,string(got),routineID)
			}
		}
	}

	wait_ch <- routineID
	//fmt.Println("Completed testcase for:",routineID)
}

func TestCase1(t *testing.T) {

	//fmt.Println("Testcases batch 1")
	
	n:=1

	var testcases = []TestCase {
		{"set dushyant 200 10\r\ngulf-talent\r\n","OK 1001\r\n",false},
		{"set ravi 1 11\r\nyodlee-tech\r\n","OK 1002\r\n",false},
		{"set rahul 100 9 noreply\r\ndb-phatak\r\n","",true},
		{"delete raavi\r\n","ERRNOTFOUND\r\n",false},
		{"delete ravi\r\n","DELETED\r\n",false},
		{"cas dushyant 300 1001 4\r\nMSCI\r\n","OK 1004\r\n",false},
		{"getm rahul\r\n","VALUE 1003 100 9 db-phatak\r\n",false},
	}
	fireTestCases(t,n,testcases)
	
	//fmt.Println("Testcases batch 2")
	
	n=3

	testcases = []TestCase {
		/*{"delete raavi\r\n","ERRNOTFOUND\r\n",false},
		{"delete ravi\r\n","DELETED\r\n",false},
		{"cas dushyant 300 1001 4\r\nMSCI\r\n","OK 1004\r\n",false},*/
		{"getm rahul\r\n","VALUE 1003 100 9 db-phatak\r\n",false},
	}
	fireTestCases(t,n,testcases)

	//fmt.Println("Testcases batch 3")
	
	/*n=50

	testcases = []TestCase {
		/*{"delete raavi\r\n","ERRNOTFOUND\r\n",false},
		{"delete ravi\r\n","DELETED\r\n",false},
		{"cas dushyant 300 1001 4\r\nMSCI\r\n","OK 1004\r\n",false},
		{"getm dushyant\r\n","VALUE 1001 200 10 gulf-talent\r\n",false},
	}
	fireTestCases(t,n,testcases)*/
}