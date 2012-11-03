package main

import (
	"fmt"
	"log"
	"os"
	"io"
	"hash"
	"crypto/sha1"
	"path"
	"time"
)

var fileHashes map[string]string
var checkHashes map[string]string
var fileLengths map[int64][]string
var hasher hash.Hash
var dupeCount, fileCount int64

const BUFFER_SIZE = 16384
var buffer []byte

func naive(fullpath string) {
	dir, err := os.Open(fullpath)

	if err != nil {
		log.Fatal(err)
	}

	files, err := dir.Readdir(0)

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(files); i++ {
		fileName := path.Join(fullpath, files[i].Name())
		if !files[i].IsDir() {
			
			file, err := os.Open(fileName)

			if err != nil {
				log.Fatal(err)
			}

			contents := make([]byte, files[i].Size())

			file.Read(contents)

			hasher := sha1.New()
			hasher.Write(contents)

			hash := fmt.Sprintf("%x", hasher.Sum(nil))

			_, present := checkHashes[fileName]

			if present && checkHashes[fileName] != hash {
				fmt.Println("Mis-hash", fileName, hash, checkHashes[fileName])
			}

			_, exists := fileHashes[hash]
			if exists {
				dupeCount++
			}

			file.Close()
		} else {
			naive(fileName)
		}
	}
}

func lengthFirst(fullpath string) {
	dir, err := os.Open(fullpath)

	if err != nil {
		log.Fatal(err)
	}

	files, err := dir.Readdir(0)

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(files); i++ {
 		fileName := path.Join(fullpath, files[i].Name())
//		fmt.Println("Working on ", fileName)
		if !files[i].IsDir() {
			fileCount++

			_, present := fileLengths[files[i].Size()]

			if !present {
				fileLengths[files[i].Size()] = append(fileLengths[files[i].Size()], fileName)
				continue
			}
			dupeCount++
			file, err := os.Open(fileName)

			if err != nil {
				log.Fatal(err)
			}

			size, err := file.Read(buffer)
			for err != io.EOF {
//				fmt.Println("Read ", size)
				var readBytes []byte = buffer[:size]
				hasher.Write(readBytes)
				size, err = file.Read(buffer)
			}
			hash := fmt.Sprintf("%x", hasher.Sum(nil))

			_, exists := fileHashes[hash]
			if exists {
				fmt.Println(fileName, "duplicates", fileHashes[hash])
			} else {
				fileHashes[hash] = fileName
			}


			checkHashes[fileName] = hash
			file.Close()
			hasher.Reset()
		} else {
			lengthFirst(fileName)
		}
	}
}

func timeTrack(start time.Time, name string) {
        elapsed := time.Since(start)
        log.Printf("function %s took %s", name, elapsed)
}

func main() {
	fileHashes = make(map[string]string)
	checkHashes = make(map[string]string)
	fileLengths = make(map[int64][]string)
	hasher = sha1.New()
	buffer = make([]byte, BUFFER_SIZE)

	name, err := os.Getwd()

	if err != nil {
		log.Fatal(err)
	}
	
	dupeCount = 0
	fileCount = 0
	t := time.Now()
	lengthFirst(name)
	timeTrack(t, "length first")
	fmt.Println("Dupes", dupeCount, "Processed", fileCount)
}



