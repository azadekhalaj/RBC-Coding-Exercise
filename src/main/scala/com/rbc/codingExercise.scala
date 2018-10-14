package com.rbc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase._

object codingExercise {

  def extractCsv(inputFile: String): DataFrame = {
    val session = SparkSession.builder().appName("RBCCodingExercise").master("local[1]").getOrCreate()
    session.read.option("header", "true").csv(inputFile)
  }

  def extractParquet(inputFile: String): DataFrame = {
    val session = SparkSession.builder().appName("RBCCodingExercise").master("local[1]").getOrCreate()
    session.read.option("header", "true").parquet(inputFile)
  }

  def extract(customersFile: String, addressesFile: String, transactionsFile: String, format: String) = {
    format match {
      case "csv" => {
        val customerDf = extractCsv(customersFile)
        customerDf.show
        val addressDf = extractCsv(addressesFile)
        addressDf.show
        val transactionDf = extractCsv(transactionsFile).select("AccountID", "TransactionAmount", "TransactionDate")
        transactionDf.show
        (customerDf, addressDf, transactionDf)
      }
      case "parquet" => {
        val customerDf = extractParquet(customersFile)
        val addressDf = extractParquet(addressesFile)
        val transactionDf = extractParquet(transactionsFile).select("AccountID", "TransactionAmount", "TransactionDate")
        (customerDf, addressDf, transactionDf)
      }
      case _ => throw new RuntimeException("Invalid format!")
    }
  }

  def transform(customerDf: DataFrame, addressDf: DataFrame, transactionDf: DataFrame): DataFrame = {
    val transformed = customerDf.join(transactionDf, Seq("AccountID")).join(addressDf, Seq("AddressID"))
//    transformed.show
    val columnsReordered = transformed.select("AccountID", "FirstName", "LastName", "Income", "AddressID", "City", "PostalCode", "TransactionAmount", "TransactionDate")
    columnsReordered.show
    columnsReordered
  }

  def load(df: DataFrame, hdfsPath: String): Unit = {
    df.write.mode(SaveMode.Overwrite).csv(hdfsPath)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.size < 6) {
      println("Not enough arguments!")
      System.exit(0)
    }

    val hdfsNn = args(0)
    val customersFilePath = args(1)
    val addressesFilePath = args(2)
    val transactionsFilePath = args(3)
    val outputPath = args(4)
    val format = args(5)

    val (customerDf, addressDf, transactionDf) = extract(hdfsNn + customersFilePath,
      hdfsNn + addressesFilePath,
      hdfsNn + transactionsFilePath,
      format)

    val transformed = transform(customerDf, addressDf, transactionDf)

    load(transformed, hdfsNn + outputPath)
  }
}
