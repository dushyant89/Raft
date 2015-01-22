package main

import (
    "fmt"
    "net"
    "strings"
    "strconv"
    "time"
    "sync"
)

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

func main() {

   // Listen for incoming connections.
    l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
    if err != nil {
        fmt.Println("Error listening:", err.Error())
    }
    // Close the listener when the application closes.
    defer l.Close()
    
    fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)

    for {
        // Listen for an incoming connection.
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
        }

        // Handle connections in a new goroutine.
        go handleRequest(conn)

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
  
  mutex.RLock()  
    m_instance:=store[key]
  mutex.RUnlock()

  //checking if the version is zero then that means there is no such value in the map
  if(m_instance.version==0) {
      conn.Write([]byte("ERRNOTFOUND\r\n"))
    } else {
        delete(store,key)
        conn.Write([]byte("DELETED\r\n"))
    }
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
func handleRequest(conn net.Conn) {

  // Make a buffer to hold incoming data.
  buf := make([]byte, 1024)
  
  // Read the incoming connection into the buffer.
  size, err := conn.Read(buf)

  if err != nil {
    fmt.Println("Error reading:", err.Error())
  }

  buf= buf[:size]

  commands := string(buf)
  commands = strings.TrimSpace(commands)
  arrayOfCommands := strings.Split(commands," ")

  checkTimeStamp()

  //the first element of the array will always be the command name
  var noReply bool= false
  
  if(arrayOfCommands[0]=="set") {
        set(conn,arrayOfCommands[1:],&noReply)

    } else if(arrayOfCommands[0]=="cas") {
        cas(conn,arrayOfCommands[1:],&noReply)

    } else if(arrayOfCommands[0]=="get") {
        get(conn,arrayOfCommands[1:])

    } else if(arrayOfCommands[0]=="getm") {
        getm(conn,arrayOfCommands[1:]) 

    } else if(arrayOfCommands[0]=="delete") {
        deleteEntry(conn,arrayOfCommands[1:]) 

    } else {
        conn.Write([]byte("ERRCMDERR\r\n"))
    }
    
    // Close the connection when you're done with it.
    conn.Close()
}