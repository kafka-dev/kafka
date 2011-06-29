/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package kafka

import (
  "container/list"
  "os"
)


type BrokerPublisher struct {
  broker *Broker
}

func NewBrokerPublisher(hostname string, topic string, partition int) *BrokerPublisher {
  return &BrokerPublisher{broker: newBroker(hostname, topic, partition)}
}


func (b *BrokerPublisher) Publish(message *Message) (int, os.Error) {
  messages := list.New()
  messages.PushBack(message)
  return b.BatchPublish(messages)
}

func (b *BrokerPublisher) BatchPublish(messages *list.List) (int, os.Error) {
  conn, err := b.broker.connect()
  if err != nil {
    return -1, err
  }
  defer conn.Close()
  // TODO: MULTIPRODUCE
  num, err := conn.Write(b.broker.EncodePublishRequest(messages))
  if err != nil {
    return -1, err
  }

  return num, err
}
