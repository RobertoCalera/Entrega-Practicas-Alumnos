package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def leerDatosDeBBDD(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enriquecimientoDispositivo_join_usuarioMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def calcularTotalBytesPorAntena(dataFrame: DataFrame): DataFrame
  def calcularTotalBytesPorEmail(dataFrame: DataFrame): DataFrame
  def calcularTotalBytesPorApp(dataFrame: DataFrame): DataFrame
  def UsuariosExcedenCuota(dataFrame: DataFrame): DataFrame


  //def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame

  //def computePercentStatusByID(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, tabla_user_metadata, tabla_bytes_hourly, tabla_user_quota_limit, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val dispositivosLeidosDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val user_metadata_DF = leerDatosDeBBDD(jdbcUri, tabla_user_metadata, jdbcUser, jdbcPassword)
    val dispositivo_join_tabla_metadataDF = enriquecimientoDispositivo_join_usuarioMetadata(dispositivosLeidosDF, user_metadata_DF).cache()

    val TotalBytesPorAntenaDF = calcularTotalBytesPorAntena(dispositivo_join_tabla_metadataDF)
    val TotalBytesPorEmailDF = calcularTotalBytesPorEmail(dispositivo_join_tabla_metadataDF)
    val TotalBytesPorAppDF = calcularTotalBytesPorApp(dispositivo_join_tabla_metadataDF)
    val UsuariosExcedenCuotaDF = UsuariosExcedenCuota(dispositivo_join_tabla_metadataDF)

    writeToJdbc(TotalBytesPorAntenaDF, jdbcUri, tabla_bytes_hourly, jdbcUser, jdbcPassword)
    writeToJdbc(TotalBytesPorEmailDF, jdbcUri, tabla_bytes_hourly, jdbcUser, jdbcPassword)
    writeToJdbc(TotalBytesPorAppDF, jdbcUri, tabla_bytes_hourly, jdbcUser, jdbcPassword)
    writeToJdbc(UsuariosExcedenCuotaDF, jdbcUri, tabla_user_quota_limit, jdbcUser, jdbcPassword)

    writeToStorage(dispositivosLeidosDF, storagePath)

    spark.close()
  }

}
