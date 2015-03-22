package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	"time"
	//"math/rand"
	"fmt"
)

/**
 TODO
	Need to protect the access to the raft object using locks
**/

type ErrRedirect int // See Log.Append. Implements Error interface.

//defining the mutex to be used for RW operation for multiple clients
var mutex = &sync.RWMutex{}

//adding wait group for synchronization between the go routines
var wg sync.WaitGroup

type SharedLog interface {
// Each data item is wrapped in a LogEntry with a unique
// lsn. The only error that will be returned is ErrRedirect,
// to indicate the server id of the leader. Append initiates
// a local disk write and a broadcast to the other replicas,
// and returns without waiting for the result.
	Append(data []byte) (LogEntity, error)
}

// --------------------------------------

//structure for log entry
type LogEntity struct {
	logIndex int
	Term int
	Data []byte
	committed bool
}

// Raft setup
type ServerConfig struct {
	Id int // Id of server. Must be unique
	Host string // name or ip of host
	ClientPort int // port at which server listens to client messages.
	LogPort int // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
 type Raft struct {
	// .... fill
	//following upper camel casing since variables will be required outside the package as well
	Clusterconfig ClusterConfig
	CommitCh chan LogEntity
	CurrentTerm int
	ServerId int
	leaderId int
	VotedFor int
	Log []LogEntity
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	filePath string
	State string
	Timer *time.Timer
}

type VoteRequestStruct struct {
	Term int
	CandidateId int
	LastLongIndex int
	Log []LogEntity //sending the log across the rpc to compare as to see whose log is more complete
}

var ackCount= make(chan int)
var voteCount= make(chan int)

func (raft Raft) sendAppendRpc(value ServerConfig,logEntity LogEntity) {
	//not to send the append entries rpc to the leader itself 
	//fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))	
	
	mutex.Lock()
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	 
	 if err != nil {
		log.Print("Error Dialing :", err)
	 }
	var args *LogEntity
	
	// Synchronous call
	args = &logEntity
	 
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client.Call("Temp.AcceptLogEntry", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:Append RPC:", err)
	}

	if reply {
		 ackCount <- 1
	}
	mutex.Unlock()
}

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {

	//fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					len(raft.Log),
					raft.CurrentTerm,
					data,
					false,
					}
	//locking the access to the log of leader for multiple clients
	mutex.Lock()
	raft.Log=append(raft.Log,logEntity)

	cc := raft.Clusterconfig
	count:= 1

	for _,value := range cc.Servers {
		if value.Id != raft.ServerId {
			go raft.sendAppendRpc(value,logEntity)
			count= count + <-ackCount
		}

		if count > len(cc.Servers)/2 { 
			//the majority acks have been received, proceed to process and commit
			raft.CommitCh <- logEntity			
			return logEntity,nil
		}
	}

	mutex.Unlock()
	return logEntity,nil
}

func (raft *Raft) sendVoteRequestRpc(value ServerConfig) {

	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	
	fmt.Print("Dialing vote request rpc from:",raft.ServerId)
	fmt.Println(" to:",value.Id)

	 if err != nil {
		log.Print("Error Dialing :", err)
	 }

	 logLen:= len(raft.Log)
	 var lastLongIndex int

	 if logLen >0 {
	 	lastLongIndex=logLen-1	 	
	 } else {
	 	lastLongIndex=0
	 }

	 args:= &VoteRequestStruct{
	 		raft.CurrentTerm,
	 		raft.ServerId,
	 		lastLongIndex,
	 		raft.Log,		
	 }
	
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client.Call("Temp.AcceptVoteRequest", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:Vote Request:", err)
	}

	if(reply) {
		fmt.Print("Received reply of vote request from:",value.Id)
		fmt.Println(" for:",raft.ServerId)
		voteCount <-1
	}
}

func (raft *Raft) voteRequest() {

	fmt.Println("Sending vote requests for:",raft.ServerId)

	//start the election and reset the vote count
	cc := raft.Clusterconfig

	//increment the current term and change the state to candidate and start sending the votes
	
	mutex.Lock()

	raft.CurrentTerm+=1
	raft.State="Candidate"
	count :=1

	for _,value := range cc.Servers {		
			if value.Id != raft.ServerId {	
				go raft.sendVoteRequestRpc(value)
				count= count + <- voteCount
			}
			if count > len(cc.Servers)/2  && raft.State!="Follower" {
					fmt.Println("New leader is:",raft.ServerId)
					//majority votes have been received, declare itself as the leader and start sending the heartbeats
					raft.State="Leader"
					raft.Append(make([] byte,0))
					break
			}	
	}

	mutex.Unlock()

}

func (raft *Raft) SetTimer() {
	//n:= rand.Intn(15)
	//n=n+15
	fmt.Println("Timer set for:",raft.ServerId)

	if raft.ServerId==0 {
		raft.Timer = time.NewTimer(time.Millisecond*200)
	} else if raft.ServerId==1 {
		raft.Timer = time.NewTimer(time.Millisecond*400)
	} else if raft.ServerId==2 {
		raft.Timer = time.NewTimer(time.Millisecond*550)
	} else if raft.ServerId==3 {
		raft.Timer = time.NewTimer(time.Millisecond*620)		
	} else if raft.ServerId==4 {
		raft.Timer = time.NewTimer(time.Millisecond*800)		
	}
	//waiting for the timer to expire on the channel
	<-raft.Timer.C

	fmt.Println("Timer expired for:",raft.ServerId)
	//if at all timer did expired start election
	raft.voteRequest()
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(log []LogEntity,config *ClusterConfig, thisServerId int, commitCh chan LogEntity, state string,votedFor int) (*Raft, error) {
	var raft Raft
	raft.Clusterconfig=*config
	raft.CommitCh=commitCh
	raft.ServerId=thisServerId
	raft.Log=log
	raft.State=state
	raft.VotedFor=votedFor
	return &raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}