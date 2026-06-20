package util

import (
	"io/ioutil"

	"github.com/Alorun/stonekv/kv/util/engine_util"
)

func NewTestEngines() *engine_util.Engines {
	engines := new(engine_util.Engines)
	var err error
	engines.KvPath, err = ioutil.TempDir("", "stonekv_kv")
	if err != nil {
		panic("create kv dir failed")
	}
	engines.Kv = engine_util.CreateDB(engines.KvPath, false)
	engines.RaftPath, err = ioutil.TempDir("", "stonekv_raft")
	if err != nil {
		panic("create raft dir failed")
	}
	engines.Raft = engine_util.CreateDB(engines.RaftPath, true)
	return engines
}
