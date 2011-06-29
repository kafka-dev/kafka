/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package main

import (
  "kafka"
  "flag"
  "fmt"
  "os"
)

var hostname string
var topic string
var partition int
var message string
var messageFile string

func init() {
  flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
  flag.StringVar(&topic, "topic", "test", "topic to publish to")
  flag.IntVar(&partition, "partition", 0, "partition to publish to")
  flag.StringVar(&message, "message", "", "message to publish")
  flag.StringVar(&messageFile, "messagefile", "", "read message from this file")
}

func main() {
  flag.Parse()
  fmt.Println("Publishing :", message)
  fmt.Printf("To: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerPublisher(hostname, topic, partition)

  if len(message) == 0 && len(messageFile) != 0 {
    file, err := os.Open(messageFile)
    if err != nil {
      fmt.Println("Error: ", err)
      return
    }
    stat, err := file.Stat()
    if err != nil {
      fmt.Println("Error: ", err)
      return
    }
    payload := make([]byte, stat.Size)
    file.Read(payload)
    timing := kafka.StartTiming("Sending")
    broker.Publish(kafka.NewMessage(payload))
    timing.Print()
    file.Close()
  } else {
    timing := kafka.StartTiming("Sending")
    broker.Publish(kafka.NewMessage([]byte(message)))
    timing.Print()
  }
}
