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
)

var hostname string
var topic string
var partition int
var offsets uint
var time int64

func init() {
  flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
  flag.StringVar(&topic, "topic", "test", "topic to read offsets from")
  flag.IntVar(&partition, "partition", 0, "partition to read offsets from")
  flag.UintVar(&offsets, "offsets", 1, "number of offsets returned")
  flag.Int64Var(&time, "time", -1, "timestamp of the offsets before that:  time(ms)/-1(latest)/-2(earliest)")
}


func main() {
  flag.Parse()
  fmt.Println("Offsets :")
  fmt.Printf("From: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerOffsetConsumer(hostname, topic, partition)

  offsets, err := broker.GetOffsets(time, uint32(offsets))
  if err != nil {
    fmt.Println("Error: ", err)
  }
  fmt.Printf("Offsets found: %d\n", len(offsets))
  for i := 0 ; i < len(offsets); i++ {
    fmt.Printf("Offset[%d] = %d\n", i, offsets[i])
  }
}
