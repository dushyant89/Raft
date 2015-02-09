## Assignment 1- Memcache Clone

### Description
All the basic functionalities including Get, Set, Getm, Cas & Delete are implemented and in working condition. RWMutexes are used as synchronization primitives for exclusive access to the hashmap

### Installation Instructions
<code>go get </code> github.com/dushyant89/cs733/assignment1

Two files are supposed to be there <br/>
1. Memcache_clone.go contains the code where all the commands are implemented and where server listens for the request <br/>
2. Memcache_clone_test.go contains all the test cases including commands which are fired concurrently evaluating all the necessary scenarios

To run the program only below command is needed (assuming the current directory is set to the assignment1 which has the go files)
<br/><code>go test</code>

### Note
1. Since Telnet client was not used during the development of the project so the use of "\r\n" is avoided in the commands and reply

### Todo
1. Test the project on concurrent requests in order of ten's of thousands
2. Enable the project to shield from network attacks including Denial of Service
