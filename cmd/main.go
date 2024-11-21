package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/syndtr/goleveldb/leveldb"
)

var db *leveldb.DB
var tempDb *leveldb.DB

func main() {
	dbPath := "/Users/priyank.lohariwal/Documents/poc/flockAndldb/fileDB"

	go openLevelDBWithRetry(dbPath, 100, 1*time.Second)

	r := gin.Default()

	r.GET("/get", func(c *gin.Context) {
		queriedKey := c.Query("key")

		data, readErr := dbInstance().Get([]byte(queriedKey), nil)
		if readErr != nil && readErr != leveldb.ErrNotFound {
			log.Fatal("Error reading levelDB", readErr)
		}

		c.JSON(http.StatusOK, gin.H{
			"value": string(data),
		})
	})

	r.POST("/add", func(c *gin.Context) {
		key := c.Query("key")
		value := c.Query("value")

		// Perform the LevelDB write operation
		err := dbInstance().Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Fatal("Error writing levelDB", err)
		}

		c.JSON(http.StatusOK, gin.H{
			"key":   string(key),
			"value": string(value),
		})
	})

	port := os.Getenv("PORT")
	r.Run("localhost:" + port)
}

func dbInstance() *leveldb.DB {
	if db != nil {
		fmt.Println("Using main DB instance")
		return db
	}

	if tempDb != nil {
		fmt.Println("Using temp DB instance")
		return tempDb
	}

	tempDbPath := "/Users/priyank.lohariwal/Documents/poc/flockAndldb/tempFileDB"
	var dbOpenErr error
	tempDb, dbOpenErr = leveldb.OpenFile(tempDbPath, nil)
	if dbOpenErr != nil {
		fmt.Println("Error opening tempDB also ", dbOpenErr)
		panic("can't open tempDB also")
	}
	fmt.Println("creating and using temp DB instance")
	return tempDb
}

func openLevelDBWithRetry(path string, maxRetries int, initialBackoff time.Duration) {
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		db, err = leveldb.OpenFile(path, nil)
		if err == nil {
			if tempDb != nil {
				// flushing data of tempDb to main DB after lock is acquired successfully
				batch := new(leveldb.Batch)
				iter := tempDb.NewIterator(nil, nil)
				for iter.Next() {
					key := iter.Key()
					value := iter.Value()
					batch.Put(key, value)
				}
				iter.Release()
				batchWriteErr := db.Write(batch, nil)
				if batchWriteErr != nil {
					log.Fatal("Error writing batch ", batch, batchWriteErr)
				}
				tempDb.Close()
			}
			return
		}

		// Check if the error is "resource temporarily unavailable"
		if err.Error() == "resource temporarily unavailable" {
			fmt.Printf("Attempt %d: Resource temporarily unavailable. Retrying in %v...\n", attempt, initialBackoff)

			// Wait for a short time before retrying
			time.Sleep(initialBackoff)
		} else {
			// If the error is not "resource temporarily unavailable", return it immediately
			fmt.Printf("error opening LevelDB: %v", err)
		}
	}

	// If we reach here, it means we exhausted all retry attempts
	fmt.Printf("failed to open LevelDB after %d attempts: %v", maxRetries, err)
}
