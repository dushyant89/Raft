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

//structure for log entry, is sort of incomplete will do it once
//the logic is ready for log safety
type LogEntity struct {
	logIndex int
	Term int
	Data []byte
	LeaderId int
	Committed bool
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
	Clusterconfig ClusterConfig  //info about the servers initially part of the cluster
	CommitCh chan LogEntity      //channel which has the entries ready to be committed
	CurrentTerm int              //current term of the raft
	ServerId int 				 //id of the server
	LeaderId int          		 //id of the current leader of the term	
	VotedFor int                 //id of the server which received the vote in the current term
	Log []LogEntity              //array containing the log entries
	commitIndex int              //index of the highest log entry known to be committed
	lastApplied int              //index of the log entry which is applied to the KV store
	nextIndex []int
	matchIndex []int
	filePath string  			 //path of the persistent log file	
	State string                 //current state of the raft object
	ElectionTimer *time.Timer
	HeartbeatTimer *time.Timer
	SetReset bool
}

type VoteRequestStruct struct {
	Term int
	CandidateId int
	LastLongIndex int
	Log []LogEntity //sending the log across the rpc to compare as to see whose log is more complete
}

var ackCount =make (chan int)
var voteCount =make (chan int)

func (raft Raft) sendAppendRpc(value ServerConfig,logEntity LogEntity) {
	//not to send the append entries rpc to the leader itself 
	fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))

	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	
	defer client.Close()

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
	fmt.Println("RPC reply from:",value.Host+":"+strconv.Itoa(value.LogPort)+" is ",reply)
	if reply {
		 ackCount <- 1
	}
	
}

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {

	fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					len(raft.Log),
					raft.CurrentTerm,
					data,
					raft.LeaderId,
					false,
					}
	//locking the access to the log of leader for multiple clients
	raft.Log=append(raft.Log,logEntity)

	cc := raft.Clusterconfig
	count:=1 

	for _,value := range cc.Servers {
		if value.Id != raft.ServerId {
			go raft.sendAppendRpc(value,logEntity)
			count = count + <-ackCount
		}

		if count > len(cc.Servers)/2 && len(data) >0 { 
			//the majority acks have been received (check only in case of append entries not heartbeats), proceed to process and commit
			//mark the entry as committed since acks from majority have been received
			logEntity.Committed=true
			raft.CommitCh <- logEntity			
			
			return logEntity,nil
		}
	}

	return logEntity,nil
}

func (raft *Raft) sendVoteRequestRpc(value ServerConfig) {

	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	
	defer client.Close()
	
	fmt.Println("Dialing vote request rpc from:",raft.ServerId," to:",value.Id)

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

	raft.CurrentTerm+=1
	raft.State="Candidate"
	count:=1

	for _,value := range cc.Servers {		
			if value.Id != raft.ServerId {	
				go raft.sendVoteRequestRpc(value)
				count= count + <-voteCount
			}
			if count > len(cc.Servers)/2  && raft.State!="Follower" {
					fmt.Println("New leader is:",raft.ServerId)
					//majority votes have been received, declare itself as the leader and start sending the heartbeats
					raft.State="Leader"
					raft.LeaderId=raft.ServerId
					wg.Add(1)
						go raft.Append(make([] byte,0))
					defer wg.Done()
					break
			}	
	}
	//wg.Wait()

	if(raft.State=="Leader") {
		raft.SetReset=true
		raft.SetHeartbeatTimer()
	} else {
		raft.State="Follower"
	}	
}

func (raft *Raft) SetElectionTimer() {
	//n:= rand.Intn(15)
	//n=n+15
	//fmt.Println("Timer set for:",raft.ServerId)

	if raft.ServerId==0 {
		raft.ElectionTimer = time.NewTimer(time.Millisecond*500)
	} else if raft.ServerId==1 {
		raft.ElectionTimer = time.NewTimer(time.Millisecond*750)
	} else if raft.ServerId==2 {
		raft.ElectionTimer = time.NewTimer(time.Millisecond*1000)
	} else if raft.ServerId==3 {
		raft.ElectionTimer = time.NewTimer(time.Millisecond*1200)		
	} else if raft.ServerId==4 {
		raft.ElectionTimer = time.NewTimer(time.Millisecond*1400)		
	}
	//waiting for the timer to expire on the channel
	<-raft.ElectionTimer.C

	fmt.Println("Timer expired for:",raft.ServerId)
	//if at all timer did expired start election
	raft.voteRequest()
}
/**
TODO - Firing of this timer has to be in line with the fact that when we have
requests coming from the clients then this timer should be set accordingly otherwise
followers will be flooded with rpcs
*/
func (raft *Raft) SetHeartbeatTimer() {
	
	//keep setting the timer unless the current server is the leader
	for raft.ServerId==raft.LeaderId && raft.SetReset {

		fmt.Println("Heartbeat is now set for the leader")
		raft.HeartbeatTimer = time.NewTimer(time.Millisecond*400)

		<-raft.HeartbeatTimer.C
		fmt.Println("Time for heart beats baby, by:",raft.ServerId)

		raft.Append(make([] byte,0))
	}	
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