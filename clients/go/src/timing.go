/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package kafka

import (
  "log"
  "time"
)

type Timing struct {
  label string
  start int64
  stop  int64
}

func StartTiming(label string) *Timing {
  return &Timing{label: label, start: time.Nanoseconds(), stop: 0}
}

func (t *Timing) Stop() {
  t.stop = time.Nanoseconds()
}

func (t *Timing) Print() {
  if t.stop == 0 {
    t.Stop()
  }
  log.Printf("%s took: %f ms\n", t.label, float64((time.Nanoseconds()-t.start))/1000000)
}
