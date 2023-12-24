package checksum

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
)

type CheckSumNewFunc func() hash.Hash

const (
	CHECKSUM_TYPE_MD5    uint8 = 0
	CHECKSUM_TYPE_SHA1   uint8 = iota
	CHECKSUM_TYPE_SHA256 uint8 = iota

	CHECKSUM_TYPE_BUTT uint8 = iota
)

var g_checkSumFunc [CHECKSUM_TYPE_BUTT]CheckSumNewFunc = [...]CheckSumNewFunc{
	md5.New,
	sha1.New,
	sha256.New,
}

func CheckSum(fp *os.File, hashType uint8, bufferSize uint32) ([]byte, error) {
	if hashType >= CHECKSUM_TYPE_BUTT || bufferSize == 0 || fp == nil {
		return nil, errors.ErrUnsupported
	}

	hashObj := g_checkSumFunc[hashType]()
	if hashObj == nil {
		return nil, errors.New("mem alloc failed")
	}
	num, err := io.CopyBuffer(hashObj, fp, make([]byte, bufferSize))
	if err != nil {
		return nil, err
	}

	info, err := fp.Stat()
	if err != nil {
		return nil, err
	}
	if num != info.Size() {
		return nil, errors.New("io error")
	}

	return hashObj.Sum(nil), nil
}
