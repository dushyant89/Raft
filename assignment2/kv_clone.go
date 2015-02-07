package main

import (
    "net"
    "strings"
    "strconv"
    "time"
    "sync"
    "log"
    "github.com/_dushyant/cs733/assignment2/raft"
    "encoding/json"
    "io/ioutil"
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

 //map which stores all the key-value data on the server 
 var store =make(map[string]Memcache)

 //a unique key which which auto increments every time a set or cas request is being sent
 var unique_version int64=1000

 //defining the mutex to be used for RW operation
 var mutex = &sync.RWMutex{}

 var leaderId int = 0

func main() {

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
        go spawnServers(cc,value)
    }

    for {
          //weird way of holding the main function, will change once a better solution is found
          time.Sleep(1*time.Hour)
    }
}

func spawnServers(cc raft.ClusterConfig,sc raft.ServerConfig) {

    // Listen for incoming connections.
    l, err := net.Listen(CONN_TYPE,sc.Host+":"+strconv.Itoa(sc.ClientPort))
    if err != nil {
        log.Print("Error listening:", err.Error())
    }
    // Close the listener when the application closes.
    defer l.Close()
    
    //fmt.Println("Listening on " + sc.Host+":"+strconv.Itoa(sc.ClientPort))

    for {
        // Listen for an incoming connection.
        conn, err := l.Accept()
        if err != nil {
            log.Print("Error accepting: ", err.Error())
        }

         raft_obj,_:=raft.NewRaft(&cc,sc.Id,make(chan raft.LogEntry))
        // Handle connections in a new goroutine.
        go handleRequest(conn,raft_obj)
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

// Handles incoming requests.
func handleRequest(conn net.Conn, raft_obj *raft.Raft) {

  // Close the connection when you're done with it.
  defer conn.Close()
  
  for {
      // Make a buffer to hold incoming data.
      buf := make([]byte, 1024)
      // Read the incoming connection into the buffer.
      size, err := conn.Read(buf)
      
      if err != nil {
        log.Print("Error reading:", err.Error())
      }

      buf= buf[:size]

      //if the go routine is acting as the leader
      if leaderId == raft_obj.ServerId() {

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

          }
        }  
}