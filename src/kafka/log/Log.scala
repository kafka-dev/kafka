/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic._
import java.text.NumberFormat
import java.io._
import java.nio.channels.FileChannel
import org.apache.log4j._
import kafka.message._
import kafka.utils._
import kafka.common._
import kafka.api.OffsetRequest
import java.util._

object Log {
  val FILE_SUFFIX = ".kafka"

  /**
   * Find a given range object in a list of ranges by a value in that range. Does a binary search over the ranges
   * but instead of checking for equality looks within the range. Takes the array size as an option in case
   * the array grows while searching happens
   *
   * TODO: This should move into SegmentList.scala
   */
  def findRange[T <: Range](ranges: Array[T], value: Long, arraySize: Int): Option[T] = {
    if(ranges.size < 1)
      return None

    // check out of bounds
    if(value < ranges(0).start || value > ranges(arraySize - 1).start + ranges(arraySize - 1).size)
      throw new OffsetOutOfRangeException("offset " + value + " is out of range")

    // check at the end
    if (value == ranges(arraySize - 1).start + ranges(arraySize - 1).size)
      return None

    var low = 0
    var high = arraySize - 1
    while(low <= high) {
      val mid = (high + low) / 2
      val found = ranges(mid)
      if(found.contains(value))
        return Some(found)
      else if (value < found.start)
        high = mid - 1
      else
        low = mid + 1
    }
    None
  }

  def findRange[T <: Range](ranges: Array[T], value: Long): Option[T] =
    findRange(ranges, value, ranges.length)

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically
   */
  def nameFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset) + Log.FILE_SUFFIX
  }
}

/**
 * A segment file in the log directory. Each log semgment consists of an open message set, a start offset and a size 
 */
class LogSegment(val file: File, val messageSet: FileMessageSet, val start: Long) extends Range {
  @volatile var deleted = false
  def size: Long = messageSet.highWaterMark
  override def toString() = "(file=" + file + ", start=" + start + ", size=" + size + ")"
}


/**
 * An append-only log for storing messages. 
 */
@threadsafe
class Log(val dir: File, val maxSize: Long, val flushInterval: Int) {

  private val logger = Logger.getLogger(classOf[Log])

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* The current number of unflushed messages appended to the write */
  private val unflushed = new AtomicInteger(0)

   /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(System.currentTimeMillis)

  /* The actual segments of the log */
  private[log] val segments: SegmentList[LogSegment] = loadSegments()

  /* The name of this log */
  val name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments(): SegmentList[LogSegment] = {
    // open all the segments read-only
    val accum = new ArrayList[LogSegment]
    val ls = dir.listFiles()
    if(ls != null) {
      for(file <- ls if file.isFile && file.toString.endsWith(Log.FILE_SUFFIX)) {
        if(!file.canRead)
          throw new IOException("Could not read file " + file)
        val filename = file.getName()
        val start = filename.substring(0, filename.length - Log.FILE_SUFFIX.length).toLong
        val messageSet = new FileMessageSet(file, false)
        accum.add(new LogSegment(file, messageSet, start))
      }
    }

    if(accum.size == 0) {
      // no existing segments, create a new mutable segment
      val newFile = new File(dir, Log.nameFromOffset(0))
      val set = new FileMessageSet(newFile, true)
      accum.add(new LogSegment(newFile, set, 0))
    } else {
      // there is at least one existing segment, validate and recover them/it
      // sort segments into ascending order for fast searching
      Collections.sort(accum, new Comparator[LogSegment] {
        def compare(s1: LogSegment, s2: LogSegment): Int = {
          if(s1.start == s2.start) 0
          else if(s1.start < s2.start) -1
          else 1
        }
      })
      validateSegments(accum)

      // run recovery on the final section and make it mutable
      val last = accum.remove(accum.size - 1)
      last.messageSet.close()
      logger.info("Loading the last segment in mutable mode and running recover on " + last.file.getAbsolutePath())
      val mutable = new LogSegment(last.file, new FileMessageSet(last.file, true, new AtomicBoolean(true)), last.start)
      accum.add(mutable)
    }
    new SegmentList(accum.toArray(new Array[LogSegment](accum.size)))
  }

  /**
   * Check that the ranges and sizes add up, otherwise we have lost some data somewhere
   */
  private def validateSegments(segments: ArrayList[LogSegment]) {
    lock synchronized {
      for(i <- 0 until segments.size - 1) {
        val curr = segments.get(i)
        val next = segments.get(i+1)
        if(curr.start + curr.size != next.start)
          throw new IllegalStateException("The following segments don't validate: " +
                  curr.file.getAbsolutePath() + ", " + next.file.getAbsolutePath())
      }
    }
  }

  /**
   * The number of segments in the log
   */
  def numberOfSegments: Int = segments.view.length

  /**
   * Close this log
   */
  def close() {
    lock synchronized {
      for(seg <- segments.view)
        seg.messageSet.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   * Returns the offset at which the messages are written.
   */
  def append(messages: MessageSet): Unit = {
    // validate the messages
    var numberOfMessages = 0
    for(message <- messages) {
      if(!message.isValid)
        throw new InvalidMessageException()
      numberOfMessages += 1;
    }
    // they are valid, insert them in the log
    lock synchronized {
      val segment = segments.view.last
      segment.messageSet.append(messages)
      maybeFlush(numberOfMessages)
      maybeRoll(segment)
    }
  }

  /**
   * Read from the log file at the given offset
   */
  def read(offset: Long, length: Int): MessageSet = {
    val view = segments.view
    Log.findRange(view, offset, view.length) match {
      case Some(segment) => segment.messageSet.read((offset - segment.start), length)
      case _ => MessageSet.Empty
    }
  }

  /**
   * Delete any log segments matching the given predicate function
   */
  def markDeletedWhile(predicate: LogSegment => Boolean): Seq[LogSegment] = {
    lock synchronized {
      val view = segments.view
      val deletable = view.takeWhile(predicate)
      for(seg <- deletable)
        seg.deleted = true
      val numToDelete = deletable.size
      // if we are deleting everything, create a new empty segment
      if(numToDelete == view.size)
        roll()
      segments.trunc(numToDelete)
    }
  }

  /**
   * Get the size of the log in bytes
   */
  def size: Long =
    segments.view.foldLeft(0L)(_ + _.size)

  /**
   * The byte offset of the message that will be appended next.
   */
  def nextAppendOffset: Long = {
    flush
    val last = segments.view.last
    last.start + last.size
  }

  /**
   *  get the current high watermark of the log
   */
  def getHighwaterMark: Long = segments.view.last.messageSet.highWaterMark

  /**
   * Roll the log over if necessary
   */
  private def maybeRoll(segment: LogSegment) {
    if(segment.messageSet.sizeInBytes > maxSize)
      roll()
  }

  /**
   * Create a new segment and make it active
   */
  def roll() {
    lock synchronized {
      val last = segments.view.last
      val newOffset = nextAppendOffset
      val newFile = new File(dir, Log.nameFromOffset(newOffset))
      if(logger.isDebugEnabled)
        logger.debug("Rolling log '" + name + "' to " + newFile.getName())
      segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset))
    }
  }

  /**
   * Flush the log if necessary
   */
  private def maybeFlush(numberOfMessages : Int) {
    if(unflushed.addAndGet(numberOfMessages) >= flushInterval) {
      flush()
    }
  }

  /**
   * Flush this log file to the physical disk
   */
  def flush() = {
    lock synchronized {
      if(logger.isDebugEnabled)
        logger.debug("Flushing log '" + name + "' last flushed: " + getLastFlushedTime + " current time: " +
          System.currentTimeMillis)
      segments.view.last.messageSet.flush()
      unflushed.set(0)
      lastflushedTime.set(System.currentTimeMillis)
     }
  }

  def getOffsetsBefore(request: OffsetRequest): Array[Long] = {
    val segsArray = segments.view
    var startIndex = -1
    val retOffset = segsArray.last.start + segsArray.last.messageSet.highWaterMark
    request.time match {
    // TODO: Latest offset is approximate right now. Change to exact
      case OffsetRequest.LATEST_TIME =>
        if(segsArray.last.size > 0)
          startIndex = segsArray.length - 1
        else
          startIndex = segsArray.length - 2
      case OffsetRequest.EARLIEST_TIME => startIndex = 0
      case _ =>
        if (request.time >= 0) {
          var isFound = false
          startIndex = segsArray.length - 1
          while (startIndex >= 0 && !isFound) {
            if (segsArray(startIndex).file.lastModified <= request.time)
              isFound = true
            else
              startIndex -=1
          }
        }
    }
    val retSize = request.maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    if(retSize > 0) {
      request.time match {
        case OffsetRequest.EARLIEST_TIME =>
          for (j <- 0 until retSize) {
            ret(j) = segsArray(startIndex).start
            startIndex -= 1
          }
        case _ =>
          ret(0) = retOffset
          for (j <- 1 until retSize) {
            ret(j) = segsArray(startIndex).start
            startIndex -= 1
          }
      }
    }
    ret
  }

  def getTopicName():String = {
    name.substring(0, name.lastIndexOf("-"))
  }

  def getLastFlushedTime():Long = {
    return lastflushedTime.get
  }
}
  
