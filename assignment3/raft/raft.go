package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	//"fmt"
)

type Lsn uint64 //Log sequence number, unique for all time.

var unique_lsn Lsn = 1000

type ErrRedirect int // See Log.Append. Implements Error interface.

//defining the mutex to be used for RW operation for multiple clients
 var mutex = &sync.RWMutex{}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}

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
	Loglsn Lsn
	Data []byte
	Committed bool
}

//LogEntity implements the LogEntry interface
/*func (le LogEntity) Lsn() Lsn {
	return le.lsn
}

func (le LogEntity) Data() []byte {
	return le.data
}

func (le LogEntity) Committed() bool {
	return le.committed
}*/

// Raft setup
type ServerConfig struct {
	Id int // Id of server. Must be unique
	Host string // name or ip of host
	ClientPort int // port at which server listens to client messages.
	LogPort int // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path string // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
 type Raft struct {
	// .... fill
	Clusterconfig ClusterConfig
	CommitCh chan LogEntity
	ServerId int
	//array of log entries maintained by each server
	Log []LogEntity
}

var ackCount =make(chan int)

func sendRpc(value ServerConfig,logEntity LogEntity) {
//not to send the append entries rpc to the leader itself 
	
	//fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))	
	
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	 
	 if err != nil {
		log.Print("Error Dialing :", err)
	 }
	 // Synchronous call
	args := &logEntity
	
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client.Call("Temp.AcceptLogEntry", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:", err)
	}
	replyCount:=0

	if(reply) {
		//fmt.Println("Received reply for:",value.Id)
		replyCount++
		ackCount <- replyCount
	}
}

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {

	//fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					unique_lsn,
					data,
					false,
					}
	//locking the access to the log of leader for multiple clients
	mutex.Lock()
	raft.Log=append(raft.Log,logEntity)

	cc := raft.Clusterconfig

	for _,value := range cc.Servers {
		if(value.Id != raft.ServerId) {
			go sendRpc(value,logEntity)
			<- ackCount
		}
	}

	//incrementing the log sequence for every append request
	unique_lsn++

	//the majority acks have been received
	raft.CommitCh <- logEntity

	//fmt.Println("Released the lock")

	mutex.Unlock()
	return logEntity,nil
}


// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntity) (*Raft, error) {
	
	var raft Raft
	raft.Clusterconfig=*config
	raft.CommitCh=commitCh
	raft.ServerId=thisServerId
	return &raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}