## Assignment 3- GO Implementation of RAFT Distributed Consensus Protocol <br/>

### Description

This is Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain a replicated state machine. The state machine is kept in sync through the use of replicated log. 

### RAFT Protocol Overview

<code>RAFT</code> protocol works for distributed systems. Which provides multiple entry points in architecture. Any server can crash at any time or there might be network partition in cluster. So single entry point will not work in case of distributed architecture. 
The way RAFT handles problem of consensus is by way of electing a leader among cluster. By which, that entry point(leader) deals with all the incoming traffic from clients. If leader by any means goes down then there will be re-election to choose the new leader and the process goes on. We make sure that safety and liveness properties are maintained throughout.
