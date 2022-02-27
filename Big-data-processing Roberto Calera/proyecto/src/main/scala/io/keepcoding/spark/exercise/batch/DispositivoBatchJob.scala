package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DispositivoBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark-Batch Proyecto Final Roberto Calera")
    .getOrCreate()

  import spark.implicits._

  // Método para leer del Storage. Se lee filtrando por la fecha y hora que se pasa como parámetro
  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }
// Aquí leemos de PostgreSQL la tabla que se recibe como parámetro
  override def leerDatosDeBBDD(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  // Join con la tabla de Usuarios
  override def enriquecimientoDispositivo_join_usuarioMetadata(dispositivoDF: DataFrame, tabla_user_quota_limitDF: DataFrame): DataFrame = {
    dispositivoDF.as("datosDispositivo")
      .join(tabla_user_quota_limitDF.as("datosBBDD"),
        $"datosDispositivo.id" === $"datosBBDD.id"
      ).drop($"datosBBDD.id")
  }

  override def calcularTotalBytesPorAntena(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("antenna_total_bytes")) // añadimos la columna "type" para poner el literal de antenna_total_bytes
      .select($"timestamp", $"antenna_id", $"bytes") // Recupero las columnas de timestamp, antena_id y los bytes enviados
      .groupBy($"antenna_id", window($"timestamp", "1 hour")) // Se agrupa por Antena porque queremos los Bytes por Antena
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por Antena
      )
      .select( $"window.start".as("timestamp"), $"antenna_id", $"value", $"type")
  }


  override def calcularTotalBytesPorEmail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("mail_total_bytes")) // añadimos la columna "type" para poner el literal de mail_total_bytes
      .select($"timestamp", $"email", $"bytes") // Recupero las columnas de timestamp, email y los bytes enviados
      .groupBy($"email", window($"timestamp", "1 hour")) // Se agrupa por email porque queremos los Bytes por email
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por email
      )
      .select( $"window.start".as("timestamp"), $"email", $"value", $"type")
  }


  override def calcularTotalBytesPorApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("app_total_bytes")) // añadimos la columna "type" para poner el literal de app_total_bytes
      .select($"timestamp", $"app", $"bytes") // Recupero las columnas de timestamp, app y los bytes enviados
      .groupBy($"app", window($"timestamp", "1 hour")) // Se agrupa por app porque queremos los Bytes por app
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por email
      )
      .select( $"window.start".as("timestamp"), $"app", $"value", $"type")
  }

  override def UsuariosExcedenCuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes", $"quota") // Recupero las columnas de timestamp, email, bytes enviados y la quota
      .groupBy($"email", window($"timestamp", "1 hour")) // Se agrupa por email porque queremos los Bytes por usuario
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados por email
      )
    .filter($"value" > $"quota") // se filtran solo los registros de los usuarios que exceden la cuota
      .select( $"email", $"value", $"quota", $"window.start".as("timestamp"))
  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  // aqui se exponen comentados los argumentos para llamar a la ejecución.
  // val Array(filterDate, storagePath, jdbcUri, tabla_user_metadata, tabla_bytes_hourly, tabla_user_quota_limit, jdbcUser, jdbcPassword) = args
  // Argumentos para ejecutar el run. Por defecto se pasa la fecha actual (now)
  // filterDate = fecha por la que queremos buscar
  // storagePath = tmp/data-spark
  // JdbcURI = jdbc:postgresql://34.88.223.246:5432/postgres
  // tabla_user_metadata = user_metadata
  // tablaBytes = bytes_hourly
  // tabla_user_quota_limit = user_quota_limit
  // jdbcUser = postgres
  // jdbcPassword = scala

  def main(args: Array[String]): Unit = run(Array(OffsetDateTime.now(), "tmp/data-spark", "jdbc:postgresql://34.88.223.246:5432/postgres", "user_metadata", "bytes_hourly", "user_quota_limit", "postgres", "scala" ))

}
