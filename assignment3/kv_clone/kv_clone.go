package main

import (
    "net"
    "net/rpc"
    "strings"
    "strconv"
    "time"
    "sync"
    "log"
    "github.com/_dushyant/cs733/assignment3/raft"
    "encoding/json"
    "io/ioutil"
    "os"
    "fmt"
)

//details of the leader hardcoded for time being
const (
    CONN_HOST = "localhost"
    CONN_PORT = "9000"
    CONN_TYPE = "tcp"
    Follower= "Follower"
    Candidate="Candidate"
    Leader="Leader"
)

type Memcache struct {
  value string
  numbytes int64
  version int64
  exptime int64
  lastLived time.Time
}
 
 //adding wait group for synchronization between the go routines
 var wg sync.WaitGroup
 
 //map which stores all the key-value data on the server 
 var store =make(map[string]Memcache)

 //a unique key which which auto increments every time a set or cas request is being sent
 var unique_version int64=1000

 //defining the mutex to be used for RW operation
 var mutex = &sync.RWMutex{}

//for starters by default making the server with id=0 as the leader
 var leaderId int = 0

 //global raft object for every server process
 var raft_obj *raft.Raft

 //temporary struct to make the rpc call to work, offcourse will think of something useful
 type Temp struct{}


func checkCommitStatus(leaderCommit int) {
    
    for i:= raft_obj.CommitIndex+1; i <= leaderCommit && i<=len(raft_obj.Log)-1; i++ {

      raft_obj.Log[i].Committed=true

      applyToStateMachine(raft_obj.Log[i].Data)

      raft_obj.File.WriteString(strconv.Itoa(raft_obj.Log[i].LogIndex)+" "+strconv.Itoa(raft_obj.Log[i].Term)+" "+strings.TrimSpace(strings.Replace(string(raft_obj.Log[i].Data),"\n"," ",-1))+" "+
        " "+strconv.FormatBool(raft_obj.Log[i].Committed))

      raft_obj.File.WriteString("\t\r\n");
    } 
    if leaderCommit > len(raft_obj.Log)-1 {
      raft_obj.CommitIndex=len(raft_obj.Log)-1
    } else {
      raft_obj.CommitIndex=leaderCommit
    }
    raft_obj.LastApplied=raft_obj.CommitIndex
}

func applyToStateMachine(buf []byte){

  commands := string(buf)
  commands = strings.TrimSpace(commands)
  lineSeparator := strings.Split(commands,"\r\n")

  //separate the command and the following value if there is any
  arrayOfCommands:= strings.Fields(lineSeparator[0])
  var newArrayOfCommands[] string
  //that means the case where there is a value, append to the array of commands
  if len(lineSeparator) >1 {
      newArrayOfCommands = make([] string,len(arrayOfCommands),len(arrayOfCommands)+1)
      copy(newArrayOfCommands,arrayOfCommands)
      newArrayOfCommands=append(newArrayOfCommands,lineSeparator[1])
  } else {
      newArrayOfCommands= make([] string,len(arrayOfCommands))
      copy(newArrayOfCommands,arrayOfCommands)
  }

  newArrayOfCommands=newArrayOfCommands[1:]
  
  checkTimeStamp()

  if(arrayOfCommands[0]=="set") {

      key:= strings.TrimSpace(newArrayOfCommands[0])
      exptime,_:= strconv.ParseInt(newArrayOfCommands[1],10,32)
      numbytes,_:= strconv.ParseInt(newArrayOfCommands[2],10,64) 
      lastLived:=time.Now().Add(time.Duration(exptime)*time.Second)

      var value string
      if(newArrayOfCommands[3]=="noreply"){
         value= strings.TrimSpace(newArrayOfCommands[4])
      } else {
        value= strings.TrimSpace(newArrayOfCommands[3])
      }
      unique_version+=1

      m_instance:= Memcache{
        value,
        numbytes,
        unique_version,
        exptime,
        lastLived,
      } 
      store[key]=m_instance

    } else if(arrayOfCommands[0]=="cas") {        
        key:= strings.TrimSpace(newArrayOfCommands[0])
        exptime,_:= strconv.ParseInt(newArrayOfCommands[1],10,32)
        version,_:= strconv.ParseInt(newArrayOfCommands[2],10,64)
        numbytes,_:= strconv.ParseInt(newArrayOfCommands[3],10,64)
        lastLived:=time.Now().Add(time.Duration(exptime)*time.Second)

        var value string

        if(newArrayOfCommands[4]=="noreply"){
          value= strings.TrimSpace(newArrayOfCommands[5])
        } else {
          value= strings.TrimSpace(newArrayOfCommands[4])
        }
        
        if(store[key].version==0) {
          
        } else if(store[key].version!=version) {
           
        } else {
                  unique_version+=1    
                  m_instance:= Memcache{
                    value,
                    numbytes,
                    unique_version,
                    exptime,
                    lastLived,
                  }
                  store[key]=m_instance
        }

    } else if(arrayOfCommands[0]=="get") {
        //nothing required to be done
    } else if(arrayOfCommands[0]=="getm") {
        //nothing required to be done
    } else if(arrayOfCommands[0]=="delete") {
        key:=strings.TrimSpace(newArrayOfCommands[0])
        m_instance:=store[key]
        //checking if the version is zero then that means there is no such value in the map
        if(m_instance.version==0) {
            
          } else {
                delete(store,key)
          }  
    }
}

func (t *Temp) AcceptLogEntry(request *raft.AppendRPCRequest, response *raft.AppendRPCResponse) error {      
        mutex.Lock()
        if raft_obj.State==Candidate {
            //convert to follower since an append rpc or heartbeat has been received
            raft_obj.State=Follower
        } 
        
        //reset the timer for the follower
        go raft_obj.ResetTimer()
        
        if request.LeaderCommit > raft_obj.CommitIndex {
            go checkCommitStatus(request.LeaderCommit)
        }

        //last log index for the raft object
        var logIndex int
        
        if len(raft_obj.Log) >0 {
            logIndex=len(raft_obj.Log)-1
        } else {
            logIndex=len(raft_obj.Log)
        }

        //appending the log entry to followers log
        if len(request.Entry.Data)!=0 {
          fmt.Println("Received append rpc for",raft_obj.ServerId)
              if request.Term < raft_obj.CurrentTerm {
                //leader's term is less than follower's term
                  fmt.Println("leader's term is less than follower's term")
                  response.Reply=false
              } else if logIndex < request.PrevLogIndex {
                //leader needs to send previous entries first to decide
                  response.Reply=false
                  response.NextIndex=logIndex
                  response.MatchIndex=-1
                  fmt.Println("leader needs to send previous entries first to decide ",logIndex," ",request.PrevLogIndex)
              } else if logIndex > request.PrevLogIndex {
                //follower has extra entries, it might be the ones which are not on the majority systems
                  if raft_obj.Log[request.PrevLogIndex].Term != request.PrevLogTerm {
                   //follower's log contains an entry at previous log index with an entry mismatch
                     response.Reply=false 
                     response.NextIndex=request.PrevLogIndex-1
                     response.MatchIndex=-1
                     fmt.Println("follower's log contains an entry at previous log index with an entry mismatch")
                  } else {
                      //override the existing entry
                      raft_obj.Log[request.PrevLogIndex]=request.Entry
                      response.Reply=true
                      response.NextIndex=request.PrevLogIndex+1
                      response.MatchIndex=response.NextIndex
                  }
              } else if logIndex == request.PrevLogIndex  {
                //length of the log is same, no issues
                  if logIndex!=0 && raft_obj.Log[logIndex].Term != request.PrevLogTerm {
                   //follower's log contains an entry at previous log index with an entry mismatch
                     response.Reply=false
                     response.NextIndex=request.PrevLogIndex-1
                     response.MatchIndex=-1
                     fmt.Println("follower's log contains an entry at previous log index with an entry mismatch")
                  } else {
                    //an ideal scenario just append the entry
                      raft_obj.Log=append(raft_obj.Log,request.Entry)
                      response.Reply=true
                      response.NextIndex=request.PrevLogIndex+1
                      response.MatchIndex=response.NextIndex
                  }
              }
          } else {
              //this is a case of heartbeat sent by the leader
              //change the votedFor value
              //updating the leaderId of the follower as well
              //updating the current term as well of the follower
              //just an additional measure to make sure old heartbeats are not sent
              if(raft_obj.CurrentTerm < request.Term) {  
                  raft_obj.CurrentTerm=request.Term
                  raft_obj.LeaderId=request.LeaderId
              }  
              
              if raft_obj.VotedFor > -1 {
                    raft_obj.VotedFor=-1
              }
              response.Reply=true
              response.NextIndex=-1
              response.MatchIndex=-1
          }

      mutex.Unlock()    
      return nil
}

func (t *Temp) AcceptVoteRequest(request *raft.VoteRequestStruct, reply *bool) error { 

    if(raft_obj.State==Follower) {

        //reset the timer for the follower
        raft_obj.ResetTimer()
        
        fmt.Println("Timer is reset via vote request for:",raft_obj.ServerId)      
          
          if raft_obj.CurrentTerm > request.Term || raft_obj.VotedFor >-1 {
              *reply=false
          } else if raft_obj.VotedFor== -1 {
              //check for the log matching property if the server has not voted yet
              //1. check for the length of the log
              //2. terms which the last entries of the log contain
              lastIndexVoter:= len(raft_obj.Log)
              lastIndexCandidate:=request.LastLongIndex

              if lastIndexVoter >0 && lastIndexCandidate>0 {
                  if raft_obj.Log[lastIndexVoter-1].Term > request.Log[lastIndexCandidate].Term {
                      *reply=false
                  } else if raft_obj.Log[lastIndexVoter-1].Term == request.Log[lastIndexCandidate].Term  {
                      if lastIndexVoter >lastIndexCandidate {
                         *reply=false
                      } else {
                        *reply=true
                      }
                  } else {
                      *reply=true
                    }
              } else if lastIndexVoter >lastIndexCandidate {
                *reply=false
              } else {
                *reply=true
              }
          } else {
              *reply=false
          }
          if(*reply) {
              raft_obj.VotedFor=request.CandidateId
          }

      } else {
          *reply=false
      }
      fmt.Println("and the reply is:",*reply)
    return nil
}

var cal=new(Temp)

func main() {

    //fetching the serverId from the command line args
    serverId, err := strconv.Atoi(os.Args[1])

    var cc raft.ClusterConfig

    //reading the file and parsing the json data
    file, err := ioutil.ReadFile("c:/Go/src/github.com/_dushyant/cs733/assignment3/server.json")
    if err != nil {
        log.Fatal(err)
    }
    err = json.Unmarshal(file, &cc)
    if err != nil {
        log.Fatal(err)
    }

    servers:= cc.Servers     //array containing the details of the servers to start at different ports

    filePath:="log_"+strconv.Itoa(serverId)+".txt"  //file path for the persistent log

    f, err := os.Create(filePath)

    if(err!=nil) {
        log.Fatal("Error creating log file:",err)
    }

    //creating the new raft object here
    raft_obj,_=raft.NewRaft(make([] raft.LogEntity,0),&cc,serverId,make(chan raft.LogEntity),Follower,-1,f)

    fmt.Println("Created the raft object for:",raft_obj.ServerId)

    for _, value := range servers {
      if value.Id == serverId  {
            wg.Add(1)
            go spawnServers(cc,value)
            defer wg.Done()
         } 
    }
    //waiting for the go rouines to finish execution
    wg.Wait()
}

func spawnServers(cc raft.ClusterConfig,sc raft.ServerConfig) {

    wg.Add(2)
    
    // Listen for incoming connections from servers
    go listenForServers(sc,strconv.Itoa(sc.LogPort))
    defer wg.Done()

    // Listen for incoming connections from clients
    go listenForClients(cc,sc,strconv.Itoa(sc.ClientPort))
    defer wg.Done()

    wg.Wait()
    
}

func listenForClients(cc raft.ClusterConfig,sc raft.ServerConfig,port string){

    l, err:= net.Listen(CONN_TYPE,sc.Host+":"+port)

    fmt.Println("Listening clients on:",sc.Host+":"+port)

    if err != nil {
        log.Print("Error listening:", err.Error())
    }
    // Close the listener when the application closes.
    defer l.Close()

    for {
          // Listen for an incoming connection.
          conn, err := l.Accept()
          
          defer conn.Close()

          if err != nil {
              log.Print("Error accepting Connection Requests: ", err.Error())
          }        
          // Handle connections in a new goroutine only for the leader
          go handleRequest(conn)
        }
}

func listenForServers(sc raft.ServerConfig,port string) {

    rpc.Register(cal)

    l, err:= net.Listen(CONN_TYPE,sc.Host+":"+port)

    //set the election timer for the server
    go raft_obj.SetElectionTimer()

    fmt.Println("Listening servers on:",sc.Host+":"+port)

    if err != nil {
        log.Print("Error listening:", err.Error())
    }
    // Close the listener when the application closes.
    defer l.Close()

    for {
          // Listen for an incoming connection.
          conn, err := l.Accept()
          if err != nil {
              log.Print("Error accepting RPC's: ", err.Error())
          }        
          // serve rpc's in a new goroutine for append entries
          go rpc.ServeConn(conn)
        }
}

//sets the new key in the map
func set(conn net.Conn,commands []string,noReply *bool) {

      key:= strings.TrimSpace(commands[0])
      exptime,_:= strconv.ParseInt(commands[1],10,32)
      numbytes,_:= strconv.ParseInt(commands[2],10,64) 
      lastLived:=time.Now().Add(time.Duration(exptime)*time.Second)

      var value string
      if(commands[3]=="noreply"){
        *noReply=true
         value= strings.TrimSpace(commands[4])
      } else {
        value= strings.TrimSpace(commands[3])
      }
      unique_version+=1

      m_instance:= Memcache{
        value,
        numbytes,
        unique_version,
        exptime,
        lastLived,
      }
      
      mutex.Lock()
        store[key]=m_instance
      mutex.Unlock()  
        
      if(!(*noReply)) {
          conn.Write([]byte("OK "+strconv.FormatInt(unique_version,10)+"\r\n"))
          *noReply=false
      }
}

//compare and the swap the key in the map
func cas(conn net.Conn,commands []string,noReply *bool){
    
    key:= strings.TrimSpace(commands[0])
    exptime,_:= strconv.ParseInt(commands[1],10,32)
    version,_:= strconv.ParseInt(commands[2],10,64)
    numbytes,_:= strconv.ParseInt(commands[3],10,64)
    lastLived:=time.Now().Add(time.Duration(exptime)*time.Second)

    var value string

    if(commands[4]=="noreply"){
      *noReply=true
      value= strings.TrimSpace(commands[5])
    } else {
      value= strings.TrimSpace(commands[4])

    }

    if(store[key].version==0) {
        conn.Write([]byte("ERRNOTFOUND\r\n"))
        *noReply=true
    } else if(store[key].version!=version) {
        conn.Write([]byte("ERR_VERSION\r\n"))
        *noReply=true 
    }

    if(!(*noReply)) {
        unique_version+=1
        m_instance:= Memcache{
          value,
          numbytes,
          unique_version,
          exptime,
          lastLived,
        }

        mutex.Lock()
          store[key]=m_instance
        mutex.Unlock()

        conn.Write([]byte("OK "+strconv.FormatInt(unique_version,10)+"\r\n"))
        *noReply=false
      }
}

//get the value from the key
func get(conn net.Conn,commands []string) {
  
  key:=strings.TrimSpace(commands[0])
  
  mutex.RLock()  
    m_instance:=store[key]
  mutex.RUnlock()

  //checking if the version is zero then that means there is no such value in the map
  if(m_instance.version==0) {
      conn.Write([]byte("ERRNOTFOUND\r\n"))
    } else {
      conn.Write([]byte("VALUE "+strconv.FormatInt(m_instance.numbytes,10) +" "+m_instance.value+"\r\n"))
    }
}

//get the metatdata from the key
func getm(conn net.Conn,commands []string){
  key:=strings.TrimSpace(commands[0])
  
  mutex.RLock()
    m_instance:=store[key]
  mutex.RUnlock()

  //checking if the version is zero then that means there is no such value in the map
  if(m_instance.version==0) {
      conn.Write([]byte("ERRNOTFOUND\r\n"))
    } else {
      conn.Write([]byte("VALUE "+strconv.FormatInt(m_instance.version,10)+" "+strconv.FormatInt(m_instance.exptime,10)+" "+
        strconv.FormatInt(m_instance.numbytes,10) +" "+m_instance.value+"\r\n"))
    }
}

//delete the entry from the map
func deleteEntry(conn net.Conn,commands []string) {
  key:=strings.TrimSpace(commands[0])
  
  mutex.Lock() 
    m_instance:=store[key]

  //checking if the version is zero then that means there is no such value in the map
  if(m_instance.version==0) {
      conn.Write([]byte("ERRNOTFOUND\r\n"))
    } else {
          delete(store,key)
          conn.Write([]byte("DELETED\r\n"))
    }
  mutex.Unlock()
}

//checks for the entries in the map which are expired
func checkTimeStamp() {
    
    for key, value := range store {
        
        now:=time.Now()
        
        if(now.After(value.lastLived) && value.exptime!=0) {
            mutex.Lock()  
              delete(store,key)
            mutex.Unlock()  
        }
    }
}

func processCommand(buf []byte,conn net.Conn,logIndex int){

  commands := string(buf)
  commands = strings.TrimSpace(commands)
  lineSeparator := strings.Split(commands,"\r\n")

  //separate the command and the following value if there is any
  arrayOfCommands:= strings.Fields(lineSeparator[0])
  var newArrayOfCommands[] string
  //that means the case where there is a value, append to the array of commands
  if len(lineSeparator) >1 {
      newArrayOfCommands = make([] string,len(arrayOfCommands),len(arrayOfCommands)+1)
      copy(newArrayOfCommands,arrayOfCommands)
      newArrayOfCommands=append(newArrayOfCommands,lineSeparator[1])
  } else {
      newArrayOfCommands= make([] string,len(arrayOfCommands))
      copy(newArrayOfCommands,arrayOfCommands)
  }   

  checkTimeStamp()

  //the first element of the array will always be the command name
  var noReply bool= false
  
  if(arrayOfCommands[0]=="set") {
        set(conn,newArrayOfCommands[1:],&noReply)
        raft_obj.LastApplied=logIndex
    } else if(arrayOfCommands[0]=="cas") {
        cas(conn,newArrayOfCommands[1:],&noReply)
        raft_obj.LastApplied=logIndex
    } else if(arrayOfCommands[0]=="get") {
        get(conn,newArrayOfCommands[1:])
    } else if(arrayOfCommands[0]=="getm") {
        getm(conn,newArrayOfCommands[1:]) 
    } else if(arrayOfCommands[0]=="delete") {
        deleteEntry(conn,newArrayOfCommands[1:]) 
        raft_obj.LastApplied=logIndex
    } else {
        conn.Write([]byte("ERRCMDERR\r\n"))
    }
}

/**
 TODO
  1. If the request is a get request so do not store 
     any of those on the state machines 
**/
// Handles incoming requests from clients only
func handleRequest(conn net.Conn) {

  fmt.Println("Handling client requests for:",conn.RemoteAddr())

  //if leader is handling the request, which most likely will be the case
  //then disable heartbeats for a moment and resume after command is processed
  if(raft_obj.State=="Leader") {
      raft_obj.SetReset=false
      raft_obj.HeartbeatTimer.Stop()
  }
  
  // Close the connection when you're done with it.
  defer conn.Close()
  
  for {
      // Make a buffer to hold incoming data.
      buf := make([]byte, 1024)
      // Read the incoming connection into the buffer.
      size, err := conn.Read(buf)

      if size > 0 {

        if err != nil {
          log.Print("Error reading:", err.Error())
        }

        //If the request has arrived to a non-Leader server
        if raft_obj.State!="Leader" {
          conn.Write([]byte("ERRREDIRECT\r\n"+" "+raft_obj.Clusterconfig.Servers[raft_obj.LeaderId].Host+":"+
            strconv.Itoa(raft_obj.Clusterconfig.Servers[raft_obj.LeaderId].ClientPort)))
          return
        }

        buf= buf[:size]

        //calling the append function for sending the append entries rpc

        go raft_obj.Append(buf)
          
        //waiting for the commit channel to send a value and then the leader can proceed
        //with unpacking the command from the client
        logentry:= <- raft_obj.CommitCh

        //fmt.Println("Now proceeding with the command processing")
        processCommand(buf,conn,logentry.LogIndex)      

      } else {
          break  //the infinite for loop
      }
    }

    raft_obj.SetReset=true
    raft_obj.SetHeartbeatTimer()
}