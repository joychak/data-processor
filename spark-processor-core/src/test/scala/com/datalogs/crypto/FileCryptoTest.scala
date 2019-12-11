package com.datalogs.crypto

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.scalatest.{FlatSpec, MustMatchers}

import scala.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.io.{Source => SIOSource}

/**
  * Created by user-1 on 2/10/17.
  */
class FileCryptoTest extends FlatSpec with MustMatchers{

  val publicKeyFileName = s"spark-processor-core/crypto-test-keys/public_key.der"
  val privateKeyFileName = s"spark-processor-core/crypto-test-keys/private_key.der"
  val bevoInpFile = "/Users/user-1/dev/idea/projects/user-1/ingestion/spark-processor-ds/data/2018-01-01_bevo_cHostname.dat"

  // Not really related to any business logic but it validates:
  // * Our generated Keys exist and work
  // * and those Keys are compatible with source code provided by Max
  // (those are building blocks for input source decryption to work)
  //ignore should "encrypt and decrypt files correctly" in {
  "The FileCrypt code" should "encrypt and decrypt files correctly" in {

    val testDir = "target/random-crypto-test"
    val fs = FileSystem.get(new Configuration)
    fs.setVerifyChecksum(false)
    fs.mkdirs(new Path(testDir))

    // Some Utilities to generate random input file(s)...
    def getFileName(fileNamePrefix : String, fileNum : Int, fileSuffix : String) = {
      s"${testDir}/${fileNamePrefix}-${fileNum}.${fileSuffix}"
    }

    def generateRandomFiles(fileNamePrefix : String, numFiles : Int = 1) : Unit = {

      def generateRandomLine = {
        (0 to Random.nextInt(50))
          .foldRight(List.empty[Char])((_, acc) => Random.nextPrintableChar() :: acc)
          .reverse
          .mkString
      }

      def generateRandomFile(fileNum : Int) = {
        val pw = new PrintWriter(getFileName(fileNamePrefix, fileNum, "txt"))
        (0 to Random.nextInt(200))
          .foreach(_ => pw.println(generateRandomLine))
        pw.close()
      }

      (1 to numFiles).foreach(generateRandomFile(_))
    }

    val fileNamePrefix = "file-crypt-test"

    // Random Input
    val numFiles = Random.nextInt(10) + 1
    generateRandomFiles(fileNamePrefix, numFiles)

    // Encrypt Input
    (1 to numFiles).foreach(ii => {

      FileCrypto.fileCrypt(
        FileCrypto.CryptoAction.ENCRYPT,
        getFileName(fileNamePrefix, ii, "txt"),
        getFileName(fileNamePrefix, ii, "enc"),
        publicKeyFileName
      )
    })

    // Decrypt Input
    (1 to numFiles).foreach(ii => {
      FileCrypto.fileCrypt(
        FileCrypto.CryptoAction.DECRYPT,
        getFileName(fileNamePrefix, ii, "enc"),
        getFileName(fileNamePrefix, ii, "dec"),
        privateKeyFileName
      )
    })

    // Compare Input with Decrypted Output
    (1 to numFiles).forall(fileNum => {
      val inpBytes = Files.readAllBytes(Paths.get(getFileName(fileNamePrefix, fileNum, "txt")))
      val decBytes = Files.readAllBytes(Paths.get(getFileName(fileNamePrefix, fileNum, "dec")))
      (inpBytes zip decBytes).forall(pair => pair._1 == pair._2)
    }) mustBe true
  }

  def generateRandomBevoFile(fileName : String) : Int = {

    val BEVO_INPUT_LINE = """{"panel_number":PANEL,"machine_number":MACHINE,"count":1,"timestamp_date":20180130,"timestamp_time":33038071,"granularity_type":0,"session_start_date":20180130,"session_start_time":32943069,"uuid":UUID,"app_name":"APPNAME","context":"TCR", "action":"changeTCRTab", "value":"ANALYTICS", "ticker":"","country_code":"ENG","metadata":{"newTab_s":"ANALYTICS","selectedColumnHeaders_ss":[],"selectedColumnLabels_ss":[],"firm_i":9001,"internal_b":true,"canDebug_b":false,"canReview_b":false,"canPreview_b":false,"hasSeenWelcomePopup_b":false,"canHaveSparklines_b":false,"canRevert_b":false,"canShare_b":true,"canClick2Sort_b":false}} """

    def generateRandomBevoInputLine : String = {
      BEVO_INPUT_LINE
        .replace("PANEL", Math.abs(Random.nextInt(5) + 1).toString)
        .replace("MACHINE", Math.abs(Random.nextInt(100)).toString)
        .replace("UUID", Math.abs(Random.nextInt(5000000)).toString)
        .replace("APPNAME", (1 to (Random.nextInt(4) + 1)).map(_ => Random.nextPrintableChar()).mkString)
    }

    // Generate a input file with empty line in the middle & end of file
    val inpFileSize = 2 // data rows
    val pw = new PrintWriter(fileName)

    // 50 random lines
    (1 to 50).foreach(rowNum => pw.println(generateRandomBevoInputLine))
    // empty line in the middle of the file
    pw.println("")
    //150 random lines
    (51 to 200).foreach(rowNum => pw.println(generateRandomBevoInputLine))
    // empty line at the end of the file
    pw.println("")
    // Randomly put extra empty line at end of file
    val extraEmptyLine = false //Random.nextBoolean()
    if (extraEmptyLine)
      pw.println("") // another empty line at the end of the file
    pw.close()

    // return size of file
    // 200 data rows, 1 empty row in middle, 1 empty row at end, 1 (optionally) extra empty line
    200 + 1 + 1 + (if (extraEmptyLine) 1 else 0)
  }

  def compareContents(decByLib : Array[String], decByUs : Array[String]) : Boolean = {
    (decByLib zip decByUs).forall((pair) => {
      val matches = (pair._1 == pair._2)
      if (!matches)
        println(s"Mismatch\n InpLine ${pair._1} DecLine ${pair._2}")
      matches
    })
  }

  //ignore should "be able to correctly decrypte an encrypted BEVO source file" in {
  "EncryptedTextRecordReader" should "be able to correctly decrypte an encrypted BEVO source file" in {

    val testDir = "target/bevo-etrr-test"
    val fs = FileSystem.getLocal(new Configuration)
    fs.mkdirs(new Path(testDir))

    val inpFileName = s"${testDir}/2018-01-01_bevo_hostname1.txt"
    val inpFileSize = generateRandomBevoFile(inpFileName)

    // Encrypt it
    val bevoEncFile = s"${testDir}/2018-01-01_bevo_hostname1.enc"

    FileCrypto.fileCrypt(
      FileCrypto.CryptoAction.ENCRYPT,
      inpFileName,
      bevoEncFile,
      publicKeyFileName
    )

    val etrr = new EncryptedTextRecordReader[LongWritable, Text](privateKeyFileName)
    val path = new Path(bevoEncFile)

    etrr.initialize(fs, path)

    def getLines (lines: Array[String]) : Array[String] = {
      val hasMoreLines = etrr.nextKeyValue
      if (hasMoreLines) {
        val (k, v) = (etrr.getCurrentKey, etrr.getCurrentValue)
        getLines(lines ++ Array(v.toString))
      }
      else {
        lines
      }
    }

    val lines = getLines(Array.empty[String])
    etrr.close

    lines.size mustBe inpFileSize

    compareContents(SIOSource.fromFile(inpFileName).getLines.toArray, lines) mustBe true
  }

  //ignore should "decrypt BEVO encrypted file correctly" in {
  "EncryptedTextInputFormat" should "decrypt BEVO encrypted file correctly" in {

    val testDir = "target/bevo-etif-test"
    val fs = FileSystem.getLocal(new Configuration)
    fs.delete(new Path(testDir), true)
    fs.mkdirs(new Path(testDir))

    val encFileNames = (1 to 50).map(fileNum => {
      val inpFileName = s"${testDir}/2018-01-01_bevo${fileNum}.txt"
      val encFileName = inpFileName.replace("txt", "enc")

      generateRandomBevoFile(inpFileName)
      FileCrypto.fileCrypt(
        FileCrypto.CryptoAction.ENCRYPT,
        inpFileName,
        encFileName,
        publicKeyFileName
      )

      encFileName
    }).mkString(",")

    val sc = DataLogsSparkSession.getOrCreate(
      master = "local[8]",
      appName = "BevoEncryptedFileTest",
      configs = HashMap[String, String]("spark.driver.allowMultipleContexts" -> "true")
    ).sparkContext

    EncryptedTextInputFormat.setPrivateKeyFileName(sc.hadoopConfiguration, privateKeyFileName)

    val hdd = sc.newAPIHadoopFile(encFileNames, classOf[EncryptedTextInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    val decryptedResults =
      hdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
        val file = inputSplit.asInstanceOf[org.apache.hadoop.mapreduce.lib.input.FileSplit]
        iterator.map {
          case (key, value) => Left(file.getPath.toString, value.toString)
          case _ => Right(null)
        }
      }.collect()

    // There shouldn't be any errors
    decryptedResults.filter(_.isRight).size mustBe 0

    // All decrypted lines should match input
    val inpFileContentsByName = encFileNames.split(",").foldLeft(Map.empty[String, Array[String]])((acc, encFileName) => {
      val inpFileName = encFileName .replace("enc", "txt")
      acc + (inpFileName -> SIOSource.fromFile(inpFileName).getLines.toArray)
    })

    encFileNames.split(",").forall(encFileName => {
      val inputFileName = encFileName.replace("enc", "txt")
      val inputContents = SIOSource.fromFile(inputFileName).getLines().toArray
      val decryptedContents = decryptedResults.map(_.left.get).filter(content => content._1 == inputFileName).map(_._2)
      val matches = compareContents(inputContents, decryptedContents)
      if (!matches)
        println(s"Mismatch\n InpFileName ${inputFileName}")
      matches
    }) mustBe true
  }
}
