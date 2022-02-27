package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JdbcProvisioner {

  def main(args: Array[String]) {
    val IpServer = "34.88.223.246"  /* IP pública del Postgre en Google Cloud */


    val driver = "org.postgresql.Driver"


    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "scala"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // Se efectua la conexión
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)


      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")
      // Se ejecuta el Create
      println("Creando la tabla metadata (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")
      println("Creando la tabla bytes (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
      println("Creando la tabla bytes_hourly (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
      println("Creando la tabla user_quota_limit (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS user_quota_limit (email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP)")


      println("Inserts de usuarios, e-mails y cuota")


      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
      statement.execute( "INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")



    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
