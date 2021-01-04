package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type GetMapperRequest struct {

}

type GetMapperResponse struct {

	NumReducer int
	FileName string
	MapperID int
	ProgressID int32

}

type DoneMapRequest struct {

	FileName string
	MapperID int
	ProgressID int32
}

type DoneMapReply struct {


}

type GetReducerRequest struct {


}

type GetReducerReply struct {

	NumMapper int
	ID int
	ProgressID int32
}

type DoneReduceRequest struct {

	ID int
	ProgressID int32
}

type DoneReduceReply struct {

}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
