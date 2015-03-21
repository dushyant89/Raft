package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	"time"
	"math/rand"
	//"fmt"
)


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
	Append(data []byte) (LogEntry, error)
}

// --------------------------------------

//structure for log entry
type LogEntity struct {
	logIndex int
	term int
	data []byte
	committed bool
}

// Raft setup
type ServerConfig struct {
	id int // Id of server. Must be unique
	host string // name or ip of host
	clientPort int // port at which server listens to client messages.
	logPort int // tcp port for inter-replica protocol messages.
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
	Timer time.Timer
}

type VoteRequestStruct struct {
	Term int
	CandidateId int
	LastLongIndex int
	Log []LogEntity //sending the log across the rpc to compare as to see whose log is more complete
}

var ackCount int =1
var voteCount int =1

func sendAppendRpc(value ServerConfig,logEntity LogEntity,heartbeat bool) {
	//not to send the append entries rpc to the leader itself 
	//fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))	
	
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	 
	 if err != nil {
		log.Print("Error Dialing :", err)
	 }
	 var args
	 // Synchronous call
	if(!heartbeat) {
		args = &logEntity
	} else {
		args = nil
	}
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client.Call("Temp.AcceptLogEntry", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:Append RPC:", err)
	}

	if(reply) {
		//fmt.Println("Received reply for:",value.Id)
		ackCount++
	}
}

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte,heartbeat bool) (LogEntity,error) {

	//fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					len(raft.Log),
					data,
					raft.CurrentTerm,
					false,
					}
	//locking the access to the log of leader for multiple clients
	mutex.Lock()
	ackCount=0
	raft.Log=append(raft.Log,logEntity)

	cc := raft.Clusterconfig

	for _,value := range cc.Servers {
		if(value.Id != raft.ServerId) {
			go sendRpc(value,logEntity)
		}
	}

	for() {
		if(ackCount > len(cc.Servers)/2) { 
			//the majority acks have been received, proceed to process and commit
			raft.CommitCh <- logEntity			
			return logEntity,nil
		}	
	}

	mutex.Unlock()
	return logEntity,nil
}

func (raft *Raft) sendVoteRequestRpc(value ServerConfig) int {

	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	 
	 if err != nil {
		log.Print("Error Dialing :", err)
	 }

	 logLen:= len(raft.Log)
	 var lastLogTerm
	 var lastLongIndex

	 if logLen >0 {
	 	lastLongIndex=logLen-1
	 	lastLogTerm=raft.Log[lastLongIndex].term
	 } else {
	 	lastLongIndex=0
	 	lastLogTerm=0
	 }

	 args:= &VoteRequestStruct{
	 		raft.currentTerm,
	 		raft.ServerId,
	 		lastLongIndex,
	 		lastLogTerm,
	 		raft.Log		
	 }
	
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client.Call("Temp.AcceptVoteRequest", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:Vote Request:", err)
	}

	if(reply) {
		//fmt.Println("Received reply for:",value.Id)
		voteCount++
	}
}

func (raft *Raft) voteRequest() {

	//start the election and reset the vote count
	cc := raft.Clusterconfig

	//increment the current term and change the state to candidate and start sending the votes
	raft.CurrentTerm+=1
	raft.State=Candidate

	for _,value := range cc.Servers {		
			if(value.id != raft.ServerId) {	
				go raft.sendVoteRequestRpc(value)
			}	
	}
	
	for {
			if(voteCount > len(cc.Servers)/2) {
				//majority votes have been received, declare itself as the leader and start sending the heartbeats
		}
	}

}

func (raft *Raft) SetTimer() {
	var n:= rand.Intn(15)
	raft.Timer = time.NewTimer(time.Millisecond*(15+n)*10)
	//waiting for the timer to expire on the channel
	<-raft.Timer.C
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