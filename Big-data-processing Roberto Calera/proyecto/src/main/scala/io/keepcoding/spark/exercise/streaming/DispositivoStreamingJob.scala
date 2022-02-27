package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.Duration

object DispositivoStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark-Streaming Proyecto Final Roberto Calera")
    .getOrCreate()

  import spark.implicits._

  // En este método nos suscribimos al topico y al servidor Kafka que se reciben como parámetros.
  // Se devuelve el dataFrame con lo que leemos del kafka
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  // Se hace el parseo de forma que se pueda recibir la estructura del Json
  // {"bytes":9082,"timestamp":1600545287,"app":"SKYPE","id":"00000000-0000-0000-0000-000000000001","antenna_id":"00000000-0000-0000-0000-000000000000"}
  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    //val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    val estructuraLeidaJson = StructType (Seq(
      StructField("bytes", IntegerType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), estructuraLeidaJson).as("value"))
      .select($"value.*")

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


  // Aquí se hace el "Join" del Kafka con la tabla, (el enriquecimiento)
  // el join es con el campo "id" de la tabla user_metadata con el "id" del Json recibido del Kafka
  override def enrichAntennaWithMetadata(dispositivosDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    dispositivosDF.as("datosUsuarioKafka")
      .join(metadataDF.as("datosBBDD"),
        $"datosUsuarioKafka.id" === $"datosBBDD.id"
      ).drop($"datosBBDD.id")
  }


  override def calculaBytesPorAntena(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("antenna_total_bytes")) // añadimos la columna "type" para poner el literal de antenna_total_bytes
      .select($"timestamp", $"antenna_id", $"bytes") // Recupero las columnas de timestamp, antena_id y los bytes enviados
      .withWatermark("timestamp", "15 seconds") // el tiempo de retardo que permitimos para recibir los mensajes
      .groupBy($"antenna_id", window($"timestamp", "5 minutes")) // Se agrupa por Antena porque queremos los Bytes por Antena
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por Antena
      )
      .select( $"window.start".as("timestamp"), $"antenna_id", $"value", $"type")
  }

  override def calculaBytesPorIdUsuario(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("user_total_bytes")) // añadimos la columna "type" para poner el literal de user_total_bytes
      .select($"timestamp", $"id", $"bytes" ) // Recupero las columnas de timestamp, id (es el usuario) y los bytes enviados
      .withWatermark("timestamp", "15 seconds") // el tiempo de retardo que permitimos para recibir los mensajes
      .groupBy($"id", window($"timestamp", "5 minutes")) // Se agrupa por id porque queremos los Bytes por Antena
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por usuario
      )
      .select( $"window.start".as("timestamp"), $"id", $"value", $"type")

  }

  override def calculaBytesPorApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("type", lit("app_total_bytes")) // añadimos la columna "type" para poner el literal de app_total_bytes
      .select($"timestamp", $"app", $"bytes") // Recupero las columnas de timestamp, app (la aplicación) y los bytes enviados
      .withWatermark("timestamp", "15 seconds") // el tiempo de retardo que permitimos para recibir los mensajes
      .groupBy($"app", window($"timestamp", "5 minutes")) // Se agrupa por id porque queremos los Bytes por Antena
      .agg(
        sum($"bytes").as("value") // se recupera la suma de los bytes enviados agrupado por usuario
      )
      .select( $"window.start".as("timestamp"), $"app", $"value", $"type")
  }



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  // aqui se exponen comentados los argumentos para llamar a la ejecución.
  // val Array(kafkaServer, topic, jdbcUri, tablaBytes, jdbcUser, jdbcPassword, storagePath) = args
  // kafkaserver = 34.69.204.250:9092
  // topic = devices
  // JdbcURI = jdbc:postgresql://34.88.223.246:5432/postgres
  // tablaBytes = bytes
  // jdbcUser = postgres
  // jdbcPassword = scala
  // storagePath = tmp/data-spark

  def main(args: Array[String]): Unit = run(Array("34.69.204.250:9092", "devices", "jdbc:postgresql://34.88.223.246:5432/postgres", "bytes", "postgres", "scala", "tmp/data-spark"))
}
