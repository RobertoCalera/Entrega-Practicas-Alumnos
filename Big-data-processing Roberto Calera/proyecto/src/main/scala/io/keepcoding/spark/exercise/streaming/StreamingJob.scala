package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame
  def parserJsonData(dataFrame: DataFrame): DataFrame
  def leerDatosDeBBDD(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def calculaBytesPorAntena(dataFrame: DataFrame): DataFrame
  def calculaBytesPorIdUsuario(dataFrame: DataFrame): DataFrame
  def calculaBytesPorApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, tablaBytes, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)  // Leemos del Kafka
    val movilesDF = parserJsonData(kafkaDF)   // Parseamos la entrada de Streaming de los dispositivos

    // En la siguiente línea hacemos la "join" del DF del Kafka con el DF del Postgre.
    // En el job de Streaming no es necesario el Join con la tabla de metadatos del Postgre, lo había hecho pero lo quito, lo dejo comentado.
    // val metadataBBDDDF = leerDatosDeBBDD(jdbcUri, tabla_user_metadata, jdbcUser, jdbcPassword)
    //val movilesEnriquecidosDF = enrichAntennaWithMetadata(movilesDF, metadataBBDDDF)
    val storageFuture = writeToStorage(movilesDF, storagePath)

    // Se calculan los DF con los Bytes Agregados por Antena, por Usuario y por App
    val bytesPorAntenaDF = calculaBytesPorAntena(movilesDF)
    val bytesPorIdUsarioDF = calculaBytesPorIdUsuario(movilesDF)
    val bytesPorAppDF = calculaBytesPorApp(movilesDF)

    // Se crean los correspondientes futuros de escritura en las tablas.
    val bytesPorAntenaFuture = writeToJdbc(bytesPorAntenaDF, jdbcUri, tablaBytes, jdbcUser, jdbcPassword)
    val bytesPorIdUsuarioFuture = writeToJdbc(bytesPorIdUsarioDF, jdbcUri, tablaBytes, jdbcUser, jdbcPassword)
    val bytesPorAppFuture = writeToJdbc(bytesPorAppDF, jdbcUri, tablaBytes, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(bytesPorAntenaFuture, storageFuture, bytesPorIdUsuarioFuture, bytesPorAppFuture)), Duration.Inf)

    spark.close()
  }

}
