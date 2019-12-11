package com.datalogs.inputformats

import com.datalogs.crypto.BigCrypto
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Created by user-1 on 2/14/17.
  */

class EncryptedTextRecordReader[K, V](privateKeyFileName : String) extends RecordReader[K, V] {

  // State
  private var initialized: Boolean = false
  private var eofReached: Boolean = false
  private var decryptedBytes: Array[Byte] = Array.empty[Byte]    // Result of last chunk of decrypted (and yet unprocessed bytes)
  private var dataLinesRead: LongWritable = new LongWritable(0L)
  private var lastDataLine: Text = null

  // Data Attributes & Utility
  private var fileName : String = "N/A"
  private var fsd : FSDataInputStream = null
  private var cryptoWrapper : BigCrypto = null
  private val inputBuffer = new Array[Byte](8 * 1024)

  private def decryptNextChunk(isInitialChunk : Boolean = false) : Array[Byte] = {

    if (eofReached) {
      null
    }
    else {
      val numBytesRead = fsd.read(inputBuffer)
      eofReached = (numBytesRead == -1)

      (isInitialChunk, eofReached) match {
        case (true, false) => cryptoWrapper.decryptInit(inputBuffer, 0, numBytesRead)
        case (true, true) => Array.empty[Byte]
        case (false, false) => cryptoWrapper.decryptUpdate(inputBuffer, 0, numBytesRead)
        case (false, true) => cryptoWrapper.decryptFinal()
      }
    }
  }

  private val DELIMITER = '\n'

  private def decryptChunksUntilDelimiter(scannedWithoutDelimiter : Array[Byte],
                                          toBeScannedForDelimiter : Array[Byte]) : (Array[Byte], Array[Byte]) = {

    val delimiterIndex = toBeScannedForDelimiter.indexOf(DELIMITER)
    if (delimiterIndex != -1) {
      val bytesWithDelimiter = toBeScannedForDelimiter.take(delimiterIndex)
      val bytesAfterDelimiter = {
        if (eofReached && (delimiterIndex == toBeScannedForDelimiter.length - 1))
          null
        else
          toBeScannedForDelimiter.drop(delimiterIndex + 1)
      }
      (scannedWithoutDelimiter ++ bytesWithDelimiter, bytesAfterDelimiter)
    }
    else {
      // DELIMITER not found in current bytes, so fetch the next chunk of bytes, inspect them ...
      val nextChunk = decryptNextChunk(false)
      if (nextChunk == null) {
        // Don't expect any more decrypted bytes, so what we have is "potentially" the last line
        // Pass it through, let the receiver (parser) trap if its busted
        (scannedWithoutDelimiter ++ toBeScannedForDelimiter, null)
      }
      else {
        decryptChunksUntilDelimiter(scannedWithoutDelimiter ++ toBeScannedForDelimiter, nextChunk)
      }
    }
  }

  // This is just refactored so that it can be unit-tested by injecting
  // our own instantiated FileSystem & Path i.e. to not rely on the
  // Hadoop generated 'split' and 'context' (as needed by the interface method)
  def initialize(fs: FileSystem, path: Path) = {
    // Initialize the decryption util
    val pkReader : FSDataInputStream = fs.open(new Path(privateKeyFileName))
    def readPrivateKeyAsBytes(bytesSoFar : Array[Byte]) : Array[Byte] = {
      val pkBytes = new Array[Byte](1024)
      val pkBytesRead = pkReader.read(pkBytes)
      if (pkBytesRead == -1)
        bytesSoFar ++ pkBytes
      else
        readPrivateKeyAsBytes(bytesSoFar ++ pkBytes)
    }
    val pkBytes : Array[Byte] = readPrivateKeyAsBytes(Array.empty[Byte])

    cryptoWrapper = new BigCrypto(null, pkBytes)

    fileName = path.toString
    fsd = fs.open(path)

    decryptedBytes = decryptNextChunk(true)
    initialized = !(decryptedBytes == null)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext) = {
    val fs = FileSystem.get(context.getConfiguration)
    val fileSplit = split.asInstanceOf[FileSplit]

    initialize(fs, fileSplit.getPath)
  }

  private def finished = (!initialized || (eofReached && decryptedBytes == null))

  override def nextKeyValue : Boolean = {
    if (initialized == false)
      false
    else if (eofReached && decryptedBytes == null) {
      // reset
      dataLinesRead = null
      lastDataLine = null
      false
    }
    else {
      val (bytesWithDelimiter, remainingDecryptedBytes) = decryptChunksUntilDelimiter(Array.empty[Byte], decryptedBytes)
      dataLinesRead = new LongWritable(dataLinesRead.get() + 1)
      decryptedBytes = remainingDecryptedBytes
      lastDataLine = new Text(bytesWithDelimiter.map(_.toChar).mkString)
      true
    }
  }

  override def getCurrentKey : K = dataLinesRead.asInstanceOf[K]

  override def getCurrentValue : V = lastDataLine.asInstanceOf[V]

  override def getProgress : Float = if (finished == true) 1.0F else 0.0F

  override def close = fsd.close()
}

class EncryptedTextInputFormat[K, V] extends FileInputFormat[K, V] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[K, V] = {
    new EncryptedTextRecordReader[K, V](EncryptedTextInputFormat.getPrivateKeyFileName(context.getConfiguration))
  }

  /**
    * Treat the encrypted file as not splittable for now
    *
    * This will simplify the handling of the file by:
    *   - process the entire file in same/single executor
    *   - incrementally read & decode chunks of data in a sequence &
    *   - work around record boundaries from decrypted data and generate actual 'lines' of data
    */
  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

object EncryptedTextInputFormat {
  final val PrivateKeyFilename = "datalogs.privatekey.filename"

  def getPrivateKeyFileName(conf: Configuration) : String = conf.get(PrivateKeyFilename)
  def setPrivateKeyFileName(conf: Configuration, privateKeyFilename: String) : Unit = conf.set(PrivateKeyFilename, privateKeyFilename)
}
