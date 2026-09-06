package main

import (
	"context"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func main() {
	consumer := NewConsumer()
	ctx := context.Background()
	go func() {
		if err := RunConsumer(ctx, consumer); err != nil {
			log.Fatalf("failed to run consumer: %v", err)
		}
	}()

	router := gin.Default()
	router.GET("/shape", func(c *gin.Context) {
		log.Printf("SHAPE LOG: reading from shape")
		table := c.Query("table")
		if table == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "table is required"})
			return
		}
		log.Printf("SHAPE LOG: table %s", table)
		offsetStr := c.Query("offset")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset"})
			return
		}
		if offset < 0 {
			offset = 0
		}
		entries, nextOffset := consumer.shapeRegistry.GetOrCreate(table).ReadFrom(offset)
		c.JSON(http.StatusOK, gin.H{"entries": entries, "nextOffset": nextOffset})
	})
	router.Run("localhost:8080")
}
