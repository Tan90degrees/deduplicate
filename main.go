package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Tan90degrees/deduplicate/checksum"
	"github.com/Tan90degrees/deduplicate/fptable"
)

const (
	RETURN_OK    int = 0
	RETURN_ERROR int = 1
)

type oneTask struct {
	sync.WaitGroup
	hashType   uint8
	bufferSize uint32
	count      atomic.Uint64
}

func (t *oneTask) checkSameAndRecord(path string) {
	fh, err := os.Open(path)
	if err != nil {
		t.Done()
		return
	}

	sum, err := checksum.CheckSum(fh, t.hashType, t.bufferSize)
	if err != nil {
		fh.Close()
		t.Done()
		return
	}

	ret := fptable.CheckSameAndInsertFpTable(fh, [8]byte(sum))
	if ret != fptable.FP_TABLE_NOT_FOUND_INSERT {
		fh.Close()
	}

	t.Done()
}

func (t *oneTask) walkDirFuncForMakeUnique(root string) {
	entrys, err := os.ReadDir(root)
	if err != nil {
		log.Println("ReadDir failed")
		t.Done()
		return
	}
	for _, entry := range entrys {
		t.count.Add(1)
		if entry.IsDir() {
			t.WaitGroup.Add(1)
			go t.walkDirFuncForMakeUnique(filepath.Join(root, entry.Name()))
		} else {
			t.WaitGroup.Add(1)
			go t.checkSameAndRecord(filepath.Join(root, entry.Name()))
		}
	}

	t.WaitGroup.Done()
}

func main() {
	root := flag.String("p", "", "path")
	flag.Parse()

	info, err := os.Stat(*root)
	if err != nil || !info.IsDir() {
		log.Println(*root, "is not a dir")
		os.Exit(RETURN_ERROR)
	}

	var task oneTask = oneTask{
		hashType:   checksum.CHECKSUM_TYPE_MD5,
		bufferSize: 8192,
	}
	fptable.InitFpTable()

	task.WaitGroup.Add(1)
	go task.walkDirFuncForMakeUnique(*root)
	task.WaitGroup.Wait()

	fptable.ExitFpTable()
	log.Println(task.count.Load())
}
