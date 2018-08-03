package io.qross.jdbc

import io.qross.util.{Properties, Wrong}

import scala.collection.{immutable, mutable}
import scala.util.control.Breaks.{break, breakable}

object JDBConnection {
    
    val PRIMARY = "mysql.qross"
    
    private val connections = new mutable.HashMap[String, JDBConnection]()
    //add primary connection
    connections += PRIMARY -> new JDBConnection(DBType.MySQL, Properties.get(PRIMARY))
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
    
    private val groups = immutable.HashMap[String, Int](
        DBType.SQLite -> DBGroup.DBS,
        DBType.MySQL -> DBGroup.RDBMS,
        DBType.SQLServer -> DBGroup.RDBMS,
        DBType.Hive -> DBGroup.DWS,
        DBType.Spark -> DBGroup.DWS,
        DBType.Impala -> DBGroup.DWS,
        DBType.Oracle -> DBGroup.RDBMS,
        //case "h2" => DBGroup.DBS
        DBType.None -> DBGroup.BASE
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
        if (Properties.contains(connectionName)) {
            connectionString = Properties.get(connectionName)
            connections += connectionName -> new JDBConnection(recognizeDBType(connectionName, connectionString), connectionString)
        }
        else if (Properties.contains(connectionName + ".url")) {
            connectionString = Properties.get(connectionName + ".url")
            connections += connectionName -> new JDBConnection(recognizeDBType(connectionName, connectionString), connectionString, Properties.get(connectionName + ".username"), Properties.get(connectionName + ".password"))
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
            for (name <- groups.keySet) {
                if (connectionName.toLowerCase().startsWith(name) || connectionString.toLowerCase().contains(name)) {
                    dbType = name
                    break
                }
            }
        }
        dbType
    }
    
    def group(connectionName: String, connectionString: String = ""): Int = {
        var dbGroup = DBGroup.BASE
        breakable {
            for (name <- groups.keySet) {
                if (connectionName.toLowerCase().startsWith(name) || connectionString.toLowerCase().contains(name)) {
                    dbGroup = groups(name)
                    break
                }
            }
        }
        dbGroup
    }
}

class JDBConnection(val dbType: String, var connectionString: String = "", var userName: String = "", var password: String = "") {
    val driver: String = JDBConnection.pickDriver(dbType)
    
    if (dbType == DBType.Oracle && userName == "" && password == "") {
        password = connectionString.substring(connectionString.lastIndexOf(":") + 1)
        connectionString = connectionString.substring(0, connectionString.lastIndexOf(":"))
        userName = connectionString.substring(connectionString.lastIndexOf(":") + 1)
        connectionString = connectionString.substring(0, connectionString.lastIndexOf(":"))
    }
}
