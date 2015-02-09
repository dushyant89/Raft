### Assignment 2- GO Implementation of RAFT Distributed Consensus Protocol <br/>

## Description

This is Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain a replicated state machine. The state machine is kept in sync through the use of replicated log. 

## RAFT Protocol Overview

<code>RAFT</code> protocol works for distributed systems. Which provides multiple entry points in architecture. Any server can crash at any time or there might be network partition in cluster. So single entry point will not work in case of distributed architecture. 
The way RAFT handles problem of consensus is by way of electing a leader among cluster. By which, that entry point(leader) deals with all the incoming traffic from clients. If leader by any means goes down then there will be re-election to choose the new leader and the process goes on. We make sure that safety and liveness properties are maintained throughout.

##Checkout Project and Directory Structure
<br/>
For checkout use following command:
<code>go get</code> github.com/dushyant89/cs733/assignment2

This is the directory structure for project:
* <b>kv_clone Directory:</b>
		kv_clone.go: This file contains the server side code. Server listens for both clients and servers 	on client port and log port respectively. After receiving the majority server pushes the changes 		on persistent log.
		kv_clone_test.go: Contains all the test cases including commands which are fired concurrently 	evaluating all the necessary scenarios.
* <b>raft Directory:</b>
		raft.go: This file has code for Raft object which is used by each server to initialize itself. It 	also contains code for method Append() which appends the log and invokes the sendRPC method th send the log to other servers.
* <b>server-spawner Directory:</b>
		server-spawner.go: For spawning multiple servers.
* <b>server.json:</b> Detail about servers is given here. It contains port number for host and log ports.


## Build and Installation Instructions
First go to “server-spawner” directory from command line and run:
<code>go install</code> <br/>
Go to “bin” directory then run <code>server-spawner</code> which will start the servers according to the configuration in server.json.
<br/>
Then go to “kv_clone” directory and run:
<code>go test </code> <br/>

## Todo
1. Project when run with multiple concurrent client configuration sometimes goes in a deadlock. So need to work on it.
