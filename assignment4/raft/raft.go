package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	"time"
	//"math/rand"
	"fmt"
	"os"
	"strings"
)

/**
 TODO
	1. Need to protect the access to the raft object using locks
	2. Making the persistent log files i.e. writing the files to disk
	3. Implementing the log safety matching property
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
	LogIndex int
	Term int
	Data []byte
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

// Raft
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
	CommitIndex int              //index of the highest log entry known to be committed
	LastApplied int              //index of the log entry which is applied to the KV store
	nextIndex []int
	matchIndex []int
	File *os.File  			 	 //handler to the persistent log file	
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

type AppendRPCRequest struct {
	Term int 		//leader's current term
	LeaderId int 	//for follower's to update themselves
	PrevLogIndex int
	PrevLogTerm int
	Entry LogEntity
	LeaderCommit int
}

type AppendRPCResponse struct {
	NextIndex int
	MatchIndex int
	Reply bool
}

var ackCount =make (chan int)
var voteCount =make (chan int)

func prepareAppendRPCRequest(raft *Raft,logEntity LogEntity) *AppendRPCRequest{
	
	var args *AppendRPCRequest

	if len(logEntity.Data)>0 {
		if len(raft.Log) >= 2 {	
			args = &AppendRPCRequest {
				raft.CurrentTerm,
				raft.LeaderId,
				len(raft.Log)-2,
				raft.Log[len(raft.Log)-2].Term,
				logEntity,
				raft.CommitIndex,
			}
		} else {
			args = &AppendRPCRequest {
				raft.CurrentTerm,
				raft.LeaderId,
				len(raft.Log)-1,
				raft.Log[len(raft.Log)-1].Term,
				logEntity,
				raft.CommitIndex,
			}
		}
	} else {
		args = &AppendRPCRequest {
				raft.CurrentTerm,
				raft.LeaderId,
				0,
				raft.CurrentTerm,
				logEntity,
				raft.CommitIndex,
			}
	}

	return args
}
/**
* TODO
*/
func (raft *Raft) sendAppendRpc(value ServerConfig,logEntity LogEntity,args *AppendRPCRequest) {
	//not to send the append entries rpc to the leader itself 
	
	if len(logEntity.Data) >0 {
		fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))
	}
		
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	
	defer client.Close()

	if err != nil {
		log.Print("Error Dialing :", err)
	}

	//this reply is the ack from the followers receiving the append entries
	var response AppendRPCResponse

	err = client.Call("Temp.AcceptLogEntry", args, &response) 

	if err != nil {
		log.Print("Remote Method Invocation Error:Append RPC:", err)
	}
	
	if len(logEntity.Data) >0 {
		fmt.Println("RPC reply from:",value.Host+":"+strconv.Itoa(value.LogPort)+" is ",response.Reply)
	}	
	if response.Reply {
		 ackCount <- 1
	} else {
		//this is necessary since we do not want the rpc call to be blocking on the channel
		//guess we need to retry the rpc untill we get positive results
		//recursively calling the append rpc method
		ackCount <- 0
	}
	
	if response.NextIndex!=-1 {
		raft.nextIndex[value.Id]=response.NextIndex
	}	
	if response.MatchIndex!=-1 {
		raft.matchIndex[value.Id]=response.MatchIndex
	}	
}

/*
TODO:
*Do we need to use channels for heartbeats to count the no of replies we have got ?
*/
//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {
	
	mutex.Lock()

	var logEntity=LogEntity{
					len(raft.Log),
					raft.CurrentTerm,
					data,
					false,
					}

	if len(data) >0 {
		//append the latest entry in the leader's log first
		fmt.Println("Ready to append the data")
		raft.Log=append(raft.Log,logEntity)
	}
	
	var args *AppendRPCRequest

	args= prepareAppendRPCRequest(raft,logEntity)

	cc := raft.Clusterconfig
	count:=1 
	done:=false
	for _,value := range cc.Servers {
		
		if value.Id != raft.ServerId {
			go raft.sendAppendRpc(value,logEntity,args)
			count = count + <-ackCount
		}
		if count > len(cc.Servers)/2 && len(data) >0 && !done{ 
			//the majority acks have been received (check only in case of append entries not heartbeats), proceed to process and commit
			//mark the entry as committed since acks from majority have been received
			done=true
			raft.CommitIndex=logEntity.LogIndex
			logEntity.Committed=true

			raft.File.WriteString(strconv.Itoa(logEntity.LogIndex)+" "+strconv.Itoa(logEntity.Term)+" "+strings.TrimSpace(strings.Replace(string(logEntity.Data),"\n"," ",-1))+" "+
		" "+strconv.FormatBool(logEntity.Committed))
			
			raft.File.WriteString("\t\r\n");

			raft.CommitCh <- logEntity
		}
	}
	mutex.Unlock()

	return logEntity,nil
}

func (raft *Raft) synchronizeFollowersLog() {
	
	cc := raft.Clusterconfig

	var args *AppendRPCRequest
	var logEntity LogEntity
	var majorityCheck int
	var heartbeat bool=false

	for _,value := range cc.Servers {
		if value.Id != raft.ServerId {
			//send entries only if the log is incomplete according to the information
			if raft.matchIndex[value.Id] < (len(raft.Log)-1) {
				fmt.Println(value.Id," ",raft.nextIndex[value.Id]," ",raft.matchIndex[value.Id]," ",len(raft.Log)-1)
				logEntity=raft.Log[raft.matchIndex[value.Id]]
				
				majorityCheck=raft.matchIndex[value.Id]
				
				args = &AppendRPCRequest {
					raft.CurrentTerm,
					raft.LeaderId,
					raft.nextIndex[value.Id]-1,
					raft.Log[raft.nextIndex[value.Id]-1].Term,
					logEntity,
					raft.CommitIndex,
				}
				heartbeat=false
			} else {
					//use it just as a heartbeat
					logEntity=LogEntity{
						len(raft.Log),
						raft.CurrentTerm,
						make([] byte,0),
						false,
					}
					args = &AppendRPCRequest {
						raft.CurrentTerm,
						raft.LeaderId,
						0,
						raft.CurrentTerm,
						logEntity,
						raft.CommitIndex,
					}
					heartbeat=true	
			}

			go raft.sendAppendRpc(value,logEntity,args)

			if 1==<-ackCount && !heartbeat{
				//now check whether the append of this entry lead to the majority
				majorityCount:=0
				for _,i:= range cc.Servers {
					if i.Id !=raft.ServerId && i!=value && raft.matchIndex[i.Id] >majorityCheck {
						majorityCount++
					}
				}
				if majorityCount < len(cc.Servers)/2 && majorityCount+1 >len(cc.Servers)/2 {
					raft.Log[raft.matchIndex[value.Id]].Committed=true
										raft.File.WriteString(strconv.Itoa(logEntity.LogIndex)+" "+strconv.Itoa(logEntity.Term)+" "+strings.TrimSpace(strings.Replace(string(logEntity.Data),"\n"," ",-1))+" "+
			" "+strconv.FormatBool(logEntity.Committed))
				
					raft.File.WriteString("\t\r\n");
				} 
			}
		}
	}

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
	} else {
		fmt.Print("Received reply of vote request from:",value.Id)
		fmt.Println(" for:",raft.ServerId)
		voteCount <-0
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
					fmt.Println("***************New leader is:",raft.ServerId,"*********************");
					//majority votes have been received, declare itself as the leader and start sending the heartbeats
					raft.State="Leader"
					raft.LeaderId=raft.ServerId
					//sending a quick heartbeat after the leader is elected
					wg.Add(1)
						go raft.Append(make([] byte,0))
					defer wg.Done()
					break
			}	
	}
	//wg.Wait()

	if(raft.State=="Leader") {
		for _,value := range cc.Servers {		
			if value.Id != raft.ServerId {
				//setting the next and match index
				raft.nextIndex[value.Id]=len(raft.Log)
				raft.matchIndex[value.Id]=0
			}
		}				
		raft.SetReset=true
		
		go raft.restrictLeadership()

		raft.SetHeartbeatTimer()
		
	} else {
		raft.State="Follower"
	}	
}

func (raft *Raft) restrictLeadership() {				
  
  for {
         if raft.State=="Leader"{
            
            <- time.After(4 * time.Second)	

            if raft.SetReset {
            	raft.State="Follower"
                raft.SetReset=false
                raft.HeartbeatTimer.Stop()
               
               	break
            }
         }     
      }  
}

func (raft *Raft) SetElectionTimer() {

	//going the hardcoded way, rand doesn't seem to work as smoothly
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

	for {
		//waiting for the timer to expire on the channel
		<-raft.ElectionTimer.C

		fmt.Println("Timer expired for:",raft.ServerId)
		//if at all timer did expired start election
		raft.voteRequest()
	}	
}

func (raft *Raft) ResetTimer() {
    if raft.ServerId==0 {
          raft.ElectionTimer.Reset(time.Millisecond*500)
        } else if raft.ServerId==1 {
          raft.ElectionTimer.Reset(time.Millisecond*750)
        } else if raft.ServerId==2 {
          raft.ElectionTimer.Reset(time.Millisecond*1000)
        } else if raft.ServerId==3 {
          raft.ElectionTimer.Reset(time.Millisecond*1200)    
        } else if raft.ServerId==4 {
          raft.ElectionTimer.Reset(time.Millisecond*1400)    
        }
}

/**
TODO - Firing of this timer has to be in line with the fact that when we have
requests coming from the clients then this timer should be set accordingly otherwise
followers will be flooded with rpcs
*/
func (raft *Raft) SetHeartbeatTimer() {
	
	//keep setting the timer unless the current server is the leader
	for raft.State=="Leader" && raft.SetReset {
		raft.HeartbeatTimer = time.NewTimer(time.Millisecond*400)

		<-raft.HeartbeatTimer.C
		
		fmt.Println("Time for heart beats baby, by:",raft.ServerId)

		raft.synchronizeFollowersLog()
	}	
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(log []LogEntity,config *ClusterConfig,thisServerId int,commitCh chan LogEntity,state string,votedFor int,fHandle *os.File) (*Raft, error) {
	var raft Raft
	raft.Clusterconfig=*config
	raft.CommitCh=commitCh
	raft.CommitIndex=-1
	raft.ServerId=thisServerId
	raft.Log=log
	raft.State=state
	raft.VotedFor=votedFor
	raft.File=fHandle
	raft.nextIndex=make([] int,5)
	raft.matchIndex=make([] int,5)
	return &raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}