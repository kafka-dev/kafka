/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package kafka


import (
  "encoding/binary"
)


func uint16bytes(value int) []byte {
  result := make([]byte, 2)
  binary.BigEndian.PutUint16(result, uint16(value))
  return result
}

func uint32bytes(value int) []byte {
  result := make([]byte, 4)
  binary.BigEndian.PutUint32(result, uint32(value))
  return result
}

func uint32toUint32bytes(value uint32) []byte {
  result := make([]byte, 4)
  binary.BigEndian.PutUint32(result, value)
  return result
}

func uint64ToUint64bytes(value uint64) []byte {
  result := make([]byte, 8)
  binary.BigEndian.PutUint64(result, value)
  return result
}
