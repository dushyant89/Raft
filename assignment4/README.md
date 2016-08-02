## Assignment 4- GO Implementation of RAFT <br/>

### Description

This is a Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain a replicated state machine. The state machine is kept in sync through the use of replicated log. It produces a result equivalent to (multi-)Paxos, and it is as efficient as Paxos, but its structure is different from Paxos; this makes Raft more understandable than Paxos and also provides a better foundation for building practical systems.

### RAFT Protocol Overview

<code>RAFT</code> protocol works for distributed systems. Which provides multiple entry points in architecture. Any server can crash at any time or there might be network partition in cluster. So single entry point will not work in case of distributed architecture. 
The way RAFT handles problem of consensus is by way of electing a leader among cluster. By which, that entry point(leader) deals with all the incoming traffic from clients. If leader by any means goes down then there will be re-election to choose the new leader and the process goes on. We make sure that safety and liveness properties are maintained throughout. Raft uses a stronger form of leadership than other consensus algorithms

### Checkout Project and Directory Structure

For checkout use following command:
<code>go get</code> github.com/dushyant89/cs733/assignment4

Below is the directory structure for project:

1. <b>kv_clone:</b>
  * kv_clone.go: This file contains the server side code. Server listens for both clients and servers on client port and log port respectively. After receiving the majority server pushes the changes on persistent log.
  * kv_clone_test.go: Contains all the test cases including commands which are fired concurrently evaluating all the necessary scenarios.
2. <b>raft:</b>
  * raft.go: This file has code for Raft object which is used by each server to initialize itself. It also contains code for methods including <code>Append()</code> which appends the log and invokes the sendRPC method to send the log to other servers and <code>voteRequest()</code> which initiates the leader election
3. <b>server-spawner:</b>
  * server-spawner.go: For spawning multiple servers as separate processes
4. <b>server.json:</b> This file contains the configuration details with which the project will start. Below are some configurable parameters
  * No. of servers for participating in the consensus protocol
  * Port no.s for listening to clients and fellow servers part of the same cluster


### Build and Installation Instructions
* Go to “kv_clone” directory from command line and run:
 <br/><code>go install </code>
* Go to “server-spawner” directory from command line and run:
<br/><code>go install</code>
* Go to “bin” directory then run <code>server-spawner</code> which will start the servers according to the configuration in server.json.
* Go to “kv_clone” directory and run for testing the assignment:
 <br/><code>go test </code>
