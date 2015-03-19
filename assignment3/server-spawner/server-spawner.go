package main

import (
	"log"
	"os"
	"os/exec"
	"sync"
)

var wg sync.WaitGroup

func handleErr(err error) {
	if err != nil {
			panic(err)
		}
}

func main() {

	path, err1 := exec.LookPath("kv_clone")
	
	if err1 != nil {
		log.Fatal("Path not set properly")
	}
		
	wg.Add(1)
	
	go func(){

		cmd := exec.Command(path, "0")
		cmd.Stdout = os.Stdout
	    cmd.Stderr = os.Stderr
		err := cmd.Run()
		handleErr(err)
		
		defer wg.Done()
	}()

	wg.Add(1)
	go func(){

		cmd := exec.Command(path, "1")
		cmd.Stdout = os.Stdout
	    cmd.Stderr = os.Stderr
		err := cmd.Run()
		handleErr(err)
		defer wg.Done()
	}()

	wg.Add(1)
	go func(){

		cmd := exec.Command(path, "2")
		cmd.Stdout = os.Stdout
	    cmd.Stderr = os.Stderr
		err := cmd.Run()
		handleErr(err)
		defer wg.Done()
	}()
 	
 	wg.Add(1)
	go func(){

		cmd := exec.Command(path, "3")
		cmd.Stdout = os.Stdout
	    cmd.Stderr = os.Stderr
		err := cmd.Run()
		handleErr(err)
		defer wg.Done()
	}()

	wg.Add(1)
	go func(){

		cmd := exec.Command(path, "4")
		cmd.Stdout = os.Stdout
	    cmd.Stderr = os.Stderr
		err := cmd.Run()
		handleErr(err)
		defer wg.Done()
	}()

 	wg.Wait()
}