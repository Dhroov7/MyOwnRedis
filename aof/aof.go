package aof

import (
	"bufio"
	"fmt"
	"io"
	"myRedis/resp"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mut  sync.RWMutex
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Error in creating or reading file", err)
		return nil, err
	}

	aof := AOF{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go func() {
		aof.mut.Lock()

		aof.file.Sync()

		aof.mut.Unlock()

		time.Sleep(time.Second)
	}()

	return &aof, nil
}

func (aof *AOF) Close() {
	aof.mut.Lock()

	defer aof.mut.Unlock()

	aof.file.Close()
}

func (aof *AOF) Write(val resp.Value) error {
	aof.mut.Lock()
	defer aof.mut.Unlock()

	_, err := aof.file.Write(val.Marshal())
	if err != nil {
		return err
	}

	return nil
}

func (aof *AOF) Read(fn func(val resp.Value)) error {
	aof.mut.Lock()
	defer aof.mut.Unlock()

	aof.file.Seek(0, io.SeekStart)

	reader := resp.NewResp(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		fn(value)
	}
	return nil
}
