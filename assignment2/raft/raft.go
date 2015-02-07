package raft

import(
"strconv"
)

type Lsn uint64 //Log sequence number, unique for all time.

type ErrRedirect int // See Log.Append. Implements Error interface.

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
	lsn Lsn
	data []byte
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
	Path string // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
 type Raft struct {
	// .... fill
	clusterConfig ClusterConfig
	commitCh chan LogEntry
	serverId int
	//array of log entries maintained by each server
	log []LogEntity
}

//Getter for the id of the server with the current raft object
func (raft Raft) ServerId() int {
	return raft.serverId
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	
	var raft Raft
	raft.clusterConfig=*config
	raft.commitCh=commitCh
	raft.serverId=thisServerId
	return &raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}