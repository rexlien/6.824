package raft

import (
	"bytes"
	"encoding/gob"
)

type Snapshot struct {

	Snapshot []byte
	Log []byte
	SnapshotIndex int
	SnapshotTerm int

}

func NewSnapshot(byte []byte) *Snapshot {
	sp := &Snapshot{}
	sp.Decode(byte)
	return sp
}

func (sp *Snapshot) Encode() []byte {

	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)

	err := encoder.Encode(sp.Snapshot)
	if err != nil {
		panic("SnapShot encode failed")
	}

	err = encoder.Encode(sp.Log)
	if err != nil {
		panic("SnapShot log state failed")
	}

	err = encoder.Encode(sp.SnapshotIndex)
	if err != nil {
		panic("SnapShot index encode failed")
	}

	err = encoder.Encode(sp.SnapshotTerm)
	if err != nil {
		panic("SnapShot index encode failed")
	}

	return b.Bytes()
}

func (sp* Snapshot) Decode(byte []byte) {

	decoder := gob.NewDecoder(bytes.NewBuffer(byte))
	err := decoder.Decode(&sp.Snapshot)
	if err != nil {
		panic("Decode failed")
	}

	err = decoder.Decode(&sp.Log)
	if err != nil {
		panic("Decode failed")
	}

	err = decoder.Decode(&sp.SnapshotIndex)
	if err != nil {
		panic("Decode failed")
	}

	err = decoder.Decode(&sp.SnapshotTerm)
	if err != nil {
		panic("Decode failed")
	}

}



//type Progress