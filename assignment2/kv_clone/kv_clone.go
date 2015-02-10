package main

import (
    "net"
    "net/rpc"
    "strings"
    "strconv"
    "time"
    "sync"
    "log"
    "github.com/_dushyant/cs733/assignment2/raft"
    "encoding/json"
    "io/ioutil"
    "os"
    //"fmt"
)

//details of the leader hardcoded for time being
const (
    CONN_HOST = "localhost"
    CONN_PORT = "9000"
    CONN_TYPE = "tcp"
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

 func (t *Temp) AcceptLogEntry(logentry *raft.LogEntity, reply *bool) error {      
      mutex.Lock()
        //appending the log entry to followers log
        raft_obj.Log = append(raft_obj.Log,*logentry)
        //fmt.Println("Append entry for:",raft_obj.ServerId," with data:",string(logentry.Data))
      mutex.Unlock()

      *reply = true
      return nil
  }

var cal=new(Temp)

func main() {

    //fetching the serverId from the command line args
    serverId, err := strconv.Atoi(os.Args[1])

    //fmt.Println(serverId)

    var cc raft.ClusterConfig

    //reading the file and parsing the json data
    file, err := ioutil.ReadFile("c:/Go/src/github.com/_dushyant/cs733/assignment2/server.json")
    if err != nil {
        log.Fatal(err)
    }
    err = json.Unmarshal(file, &cc)
    if err != nil {
        log.Fatal(err)
    }

    servers:= cc.Servers     //array containing the details of the servers to start at different ports

    for _, value := range servers {
      if(value.Id==serverId)  {
            wg.Add(1)
            go spawnServers(cc,value)
            defer wg.Done()
         } 
    }

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

    //fmt.Println("Listening clients on:",sc.Host+":"+port)

    if err != nil {
        log.Print("Error listening:", err.Error())
    }
    // Close the listener when the application closes.
    defer l.Close()
    
    //creating new raft object
    raft_obj,_=raft.NewRaft(&cc,sc.Id,make(chan raft.LogEntity))

    for {
          // Listen for an incoming connection.
          conn, err := l.Accept()
          if err != nil {
              log.Print("Error accepting Connection Requests: ", err.Error())
          }        
          // Handle connections in a new goroutine for the leader
          go handleRequest(conn)
        }
}

func listenForServers(sc raft.ServerConfig,port string) {
    
    rpc.Register(cal)

    l, err:= net.Listen(CONN_TYPE,sc.Host+":"+port)

    //fmt.Println("Listening servers on:",sc.Host+":"+port)

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

// Handles incoming requests from clients only
func handleRequest(conn net.Conn) {

  //fmt.Println("Handling connection for:",conn.RemoteAddr())
  
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

      buf= buf[:size]

      //calling the append function for sending the append entries rpc
      go raft_obj.Append(buf)

      //waiting for the commit channel to send a value and then the leader can proceed
      //with unpacking the command from the client
      <- raft_obj.CommitCh

      //fmt.Println("Now proceeding with the command processing")

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

        } else if(arrayOfCommands[0]=="cas") {
            cas(conn,newArrayOfCommands[1:],&noReply)

        } else if(arrayOfCommands[0]=="get") {
            get(conn,newArrayOfCommands[1:])

        } else if(arrayOfCommands[0]=="getm") {
            getm(conn,newArrayOfCommands[1:]) 

        } else if(arrayOfCommands[0]=="delete") {
            deleteEntry(conn,newArrayOfCommands[1:]) 

        } else {
            conn.Write([]byte("ERRCMDERR\r\n"))
        }
      } else {
          //fmt.Println("Reading EOF from conn")
      }
    }  
}