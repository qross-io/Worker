package io.qross.jdbc

import io.qross.util.{PropertiesX, Wrong}

import scala.collection.{immutable, mutable}
import scala.util.control.Breaks.{break, breakable}

object JDBConnection {
    
    val PRIMARY = "mysql.qross"
    
    private val connections = new mutable.HashMap[String, JDBConnection]()
    //add primary connection
    connections += PRIMARY -> new JDBConnection(DBType.MySQL, PropertiesX.get(PRIMARY))
    connections += "sqlite.memory" -> new JDBConnection(DBType.SQLite, "jdbc:sqlite::memory:")

    private val drivers = immutable.HashMap[String, String](
        DBType.SQLite -> "org.sqlite.JDBC",
        DBType.MySQL -> "com.mysql.jdbc.Driver",
        DBType.SQLServer -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        DBType.Hive -> "org.apache.hive.jdbc.HiveDriver",
        DBType.Spark -> "org.apache.hive.jdbc.HiveDriver",
        DBType.Impala -> "org.apache.hive.jdbc.HiveDriver",
        DBType.Oracle -> "oracle.jdbc.driver.OracleDriver",
        //case "impala" => "com.cloudera.impala.jdbc4.Driver"
        //case "h2" => "org.h2.Driver"
        DBType.None -> ""
    )

    def contains(connectionName: String): Boolean = {
        connections.contains(connectionName)
    }
    
    def get(connectionName: String): JDBConnection = {
        if (!connections.contains(connectionName)) {
            load(connectionName)
        }
        if (connections.contains(connectionName)) {
            connections(connectionName)
        }
        else {
            throw new Exception(Wrong.WRONG_CONNECTION_NAME(connectionName))
        }
    }
    
    def load(connectionName: String): Unit = {
        var connectionString = ""
        if (PropertiesX.contains(connectionName)) {
            connectionString = PropertiesX.get(connectionName)
            connections += connectionName -> new JDBConnection(recognizeDBType(connectionName, connectionString), connectionString)
        }
        else if (PropertiesX.contains(connectionName + ".url")) {
            connectionString = PropertiesX.get(connectionName + ".url")
            connections += connectionName -> new JDBConnection(recognizeDBType(connectionName, connectionString), connectionString, PropertiesX.get(connectionName + ".username"), PropertiesX.get(connectionName + ".password"))
        }
        else {
            val ds = new MySQL(PRIMARY)
            val connection = ds.executeDataRow(s"SELECT * FROM qross_connections WHERE connection_name='$connectionName'")
            if (connection.nonEmpty) {
                connections += connectionName -> new JDBConnection(
                    connection.getString("connection_type"),
                    connection.getString("connection_string"),
                    connection.getString("username"),
                    connection.getString("password")
                )
            }
            ds.close()
        }
    }
    
    def pickDriver(dbType: String): String = {
        drivers.getOrElse(dbType, "")
    }
    
    def recognizeDBType(connectionName: String, connectionString: String = ""): String = {
        var dbType = DBType.None
        breakable {
            for (name <- drivers.keySet) {
                if (connectionName.toLowerCase().startsWith(name) || connectionString.toLowerCase().contains(name)) {
                    dbType = name
                    break
                }
            }
        }
        dbType
    }
}

class JDBConnection(var dbTypeOrDriver: String, var connectionString: String = "", var userName: String = "", var password: String = "") {
    if (JDBConnection.drivers.contains(dbTypeOrDriver)) {
        dbTypeOrDriver = JDBConnection.drivers(dbTypeOrDriver)
    }
}
