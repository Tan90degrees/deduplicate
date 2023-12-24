package fptable

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type fptableItem struct {
	fh       *os.File
	refCount uint64
	sameList *list.List
}

type fptableHash struct {
	table map[[8]byte]*list.List
	lock  sync.Mutex
}

const (
	FP_TABLE_IS_SAME          uint8 = iota
	FP_TABLE_IS_NOT_SAME      uint8 = iota
	FP_TABLE_NOT_FOUND        uint8 = iota
	FP_TABLE_NOT_FOUND_INSERT uint8 = iota
	FP_TABLE_ERROR            uint8 = iota
	FP_TABLE_IO_ERROR         uint8 = iota
)

var g_fptable fptableHash

func CheckSameCompareFile(bufDst []byte, bufSrc []byte, fhDst *os.File, fhSrc *os.File) uint8 {
	var err error
	var numDst int
	var numSrc int
	var result uint8 = FP_TABLE_IS_SAME

	for err != io.EOF {
		numDst, err = fhDst.Read(bufDst)
		if err != io.EOF && err != nil {
			result = FP_TABLE_IS_NOT_SAME // todo: retry
			break
		}

		numSrc, err = fhSrc.Read(bufSrc)
		if err != io.EOF && err != nil {
			result = FP_TABLE_IS_NOT_SAME // todo: retry
			break
		}

		if (numSrc != numDst) || !bytes.Equal(bufSrc, bufDst) {
			result = FP_TABLE_IS_NOT_SAME
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Print(err)
		result = FP_TABLE_IO_ERROR
	}

	return result
}

func CheckSameAndInsertFpTable(fhSrc *os.File, fp [8]byte) uint8 {
	g_fptable.lock.Lock()
	conflictList := g_fptable.table[fp]

	if conflictList != nil && conflictList.Len() != 0 {
		bufDst := make([]byte, 8192)
		bufSrc := make([]byte, 8192)
		if bufDst == nil || bufSrc == nil {
			g_fptable.lock.Unlock()
			return FP_TABLE_ERROR
		}

		var ret uint8 = FP_TABLE_IS_NOT_SAME
		itemDstObj := conflictList.Front()
		for ; itemDstObj != nil; itemDstObj = itemDstObj.Next() {
			itemDst, ok := itemDstObj.Value.(*fptableItem)
			if !ok {
				g_fptable.lock.Unlock()
				return FP_TABLE_ERROR
			}
			clear(bufDst)
			clear(bufSrc)
			itemDst.fh.Seek(0, 0)
			fhSrc.Seek(0, 0)

			ret = CheckSameCompareFile(bufDst, bufSrc, itemDst.fh, fhSrc)
			if ret == FP_TABLE_IS_SAME {
				itemDst.sameList.PushBack(fhSrc.Name())
				itemDst.refCount++
				break
			} else if ret == FP_TABLE_IS_NOT_SAME {
				log.Printf("Got one confilict dst(%s) src(%s)\n", itemDst.fh.Name(), fhSrc.Name())
				continue
			} else {
				continue // todo: retry
			}
		}

		if ret == FP_TABLE_IS_NOT_SAME {
			conflictList.PushBack(&fptableItem{
				fh:       fhSrc,
				refCount: 1,
				sameList: list.New(),
			})
			ret = FP_TABLE_NOT_FOUND_INSERT
		}

		g_fptable.lock.Unlock()
		return ret
	} else {
		g_fptable.table[fp] = list.New()
		g_fptable.table[fp].PushBack(&fptableItem{
			fh:       fhSrc,
			refCount: 1,
			sameList: list.New(),
		})
		g_fptable.lock.Unlock()
		return FP_TABLE_NOT_FOUND_INSERT
	}
}

func InitFpTable() {
	g_fptable.table = make(map[[8]byte]*list.List)
}

func writeSameListToFile(sameList *list.List, logFh *os.File) {
	fileNameObj := sameList.Front()
	logFh.WriteString("Same file:\n")
	for ; fileNameObj != nil; fileNameObj = fileNameObj.Next() {
		fileName, ok := fileNameObj.Value.(string)
		if !ok {
			continue
		}
		logFh.WriteString(fmt.Sprintf("---%s\n", fileName))
	}
	logFh.WriteString("\n")
}

func printSameList(sameList *list.List) {
	fileNameObj := sameList.Front()
	fmt.Printf("Same file:\n")
	for ; fileNameObj != nil; fileNameObj = fileNameObj.Next() {
		fileName, ok := fileNameObj.Value.(string)
		if !ok {
			continue
		}
		fmt.Printf("---%s\n", fileName)
	}
	fmt.Printf("\n")
}

func ExitFpTable() {
	logFh, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println(err)
		logFh = nil
	}
	g_fptable.lock.Lock()

	for fp, conflictList := range g_fptable.table {
		if conflictList != nil && conflictList.Len() != 0 {
			itemObj := conflictList.Front()
			for ; itemObj != nil; itemObj = itemObj.Next() {
				item, ok := itemObj.Value.(*fptableItem)
				if !ok {
					continue
				}
				if item.sameList.Len() != 0 {
					if logFh != nil {
						logFh.WriteString(fmt.Sprintf("Base file: %s\n", item.fh.Name()))
						writeSameListToFile(item.sameList, logFh)
					} else {
						fmt.Printf("Base file: %s\n", item.fh.Name())
						printSameList(item.sameList)
					}
				}
				item.fh.Close()
			}
			delete(g_fptable.table, fp)
		}
	}

	g_fptable.lock.Unlock()
	logFh.Close()
}
