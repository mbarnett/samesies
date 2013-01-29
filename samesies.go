package main

import (
	"fmt"
	"log"
	"os"
	"io"
	"crypto/md5"
	"path"
	"sync"
	"runtime"
)

// This seems to be the optimal block size for reads on OS X 10.8
const BUFFER_SIZE = 16384

// default OS X ulimit on file descriptors is 256
// we have to keep workers strictly below this
// because go will eat all file descriptors in
// miliseconds if we let it
const WORKER_POOL = 250

type fileEntry struct {
	Path   string
	Hash   string
	Info   *os.FileInfo
}

type fileHashMap map[string][]*fileEntry
type fileLengthMap map[int64][]*fileEntry


func scanDir(dirs chan string, fileEntries chan *fileEntry, wg *sync.WaitGroup) {
	for dirpath := range dirs {
		dir, err := os.Open(dirpath)
		if err != nil {
			log.Fatal(err)
		}

		files, err := dir.Readdir(0)
		dir.Close()

		if err != nil {
			log.Fatal(err)
		}

		for i := range files {
 			fileName := path.Join(dirpath, files[i].Name())

			if !files[i].IsDir() {
				entry := new(fileEntry)
				entry.Path = fileName
				entry.Info = &files[i]

				fileEntries <- entry
			} else {
				wg.Add(1)
				dirs <- fileName
			}
		}
		wg.Done()
	}
}

func hashEntry(entries chan *fileEntry, results chan *fileEntry, wg *sync.WaitGroup) {
	buffer := make([]byte, BUFFER_SIZE)
	hasher := md5.New()

	for entry := range entries {
		file, err := os.Open((*entry).Path)

		if err != nil {
			log.Fatal(err)
		}

		size, err := file.Read(buffer)
		for err != io.EOF {
			var readBytes []byte = buffer[:size]
			hasher.Write(readBytes)
			size, err = file.Read(buffer)
		}
		(*entry).Hash = fmt.Sprintf("%x", hasher.Sum(nil))

		file.Close()
		results <- entry
		hasher.Reset()
	}
}


func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fileHashes := make(fileHashMap)
	dirs := make(chan string)
	fileEntries := make(chan *fileEntry)
	hashEntries := make(chan *fileEntry, 100)
	hashedResults := make(chan *fileEntry, 100)
	fileLengths := make(fileLengthMap)
	var wg sync.WaitGroup

	name, err := os.Getwd()

	if err != nil {
		log.Fatal(err)
	}

	go func () {
		for result := range fileEntries {
			var f os.FileInfo = *result.Info
			size := f.Size()
			fileLengths[size] = append(fileLengths[size], result)
		}
	}()
	
	for i := 0; i < WORKER_POOL; i++ { 
		go scanDir(dirs, fileEntries, &wg)
	}

	wg.Add(1)
	dirs <- name
	wg.Wait()
	close(dirs)
	close(fileEntries)

	go func() {
		for {
			result := <-hashedResults
			if fileHashes[(*result).Hash] == nil {
				fileHashes[(*result).Hash] = make([]*fileEntry,5)
			}
			fileHashes[(*result).Hash] = append(fileHashes[(*result).Hash], result)
			wg.Done()
		}
	}()

	for i := 0; i < WORKER_POOL; i++ {
		go hashEntry(hashEntries, hashedResults, &wg)
	}

	for _, entries := range fileLengths {
		if len(entries) > 1 {
			for i := range entries {
				wg.Add(1)
				hashEntries <- entries[i]
			}
		}
	}

	wg.Wait()
	close(hashEntries)
	close(hashedResults)

	for hash, files := range fileHashes {
		fmt.Println("Group", hash)
		for _, entry := range files {
		 	fmt.Println((*entry).Path)
		 }
		 fmt.Println()
	}
}