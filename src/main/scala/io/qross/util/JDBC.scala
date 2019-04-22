package io.qross.util

import scala.collection.{immutable, mutable}
import scala.util.control.Breaks.breakable

object DBType {
    val MySQL = "mysql"
    val SQLServer = "sqlserver"
    val Hive = "hive"
    val Memory = "sqlite.memory"
    val SQLite = "sqlite"
    val Oracle = "oracle"
    val PostgreSQL = "postgresql"
    val Impala = "impala"
    val Spark = "spark"
    val Presto = "presto"
}

object JDBC {

    val QROSS = "mysql.qross"

    val drivers = immutable.HashMap[String, String](
                DBType.MySQL -> "com.mysql.cj.jdbc.Driver,com.mysql.cj.jdbc.Driver",
                DBType.SQLite -> "org.sqlite.JDBC",
                DBType.Presto -> "com.facebook.presto.jdbc.PrestoDriver",
                DBType.Hive -> "org.apache.hive.jdbc.HiveDriver",
                DBType.Oracle -> "oracle.jdbc.driver.OracleDriver",
                DBType.Impala -> "org.apache.hive.jdbc.HiveDriver,com.cloudera.impala.jdbc4.Driver",
                DBType.SQLServer -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
                DBType.Memory -> "org.sqlite.JDBC"

    //已保存的数据库连接信息
    val connections = new mutable.HashMap[String, JDBC]()
    connections += DBType.Memory -> new JDBC(DBType.Memory)

    def qross: JDBC = {
        if (!connections.contains(QROSS)) {
            connections += QROSS -> new JDBC(DBType.MySQL, Properties.get(QROSS), drivers(DBType.MySQL))
        }
        connections(QROSS)
    }

    //从Properties新建
    def take(connectionName: String): JDBC = {
        var dbType = ""
        var connectionString = ""
        var driver = ""
        var username = ""
        var password = ""
        var overtime = 0
        var retry = 0


        if (Properties.contains(connectionName)) {
            connectionString = Properties.get(connectionName)
        }
        else if (Properties.contains(connectionName + ".url")) {
            connectionString = Properties.get(connectionName + ".url")
        }
        else {
            throw new Exception(s"""Can't find connection string of connection name "$connectionName"" in properties.""")
        }

        breakable {
            for (name <- JDBC.drivers.keySet) {
                if (connectionName.contains(name) || connectionString.contains(name)) {
                    dbType = name
                }
            }
        }

        if (Properties.contains(connectionName + ".driver")) {
            driver = Properties.get(connectionName + ".driver")
        }

        if (driver == "") {
            breakable {
                for (name <- JDBC.drivers.keySet) {
                    if (connectionName.contains(name) || connectionString.contains(name)) {
                        driver = JDBC.drivers(name)
                    }
                }
            }
        }

        if (driver == "") {
            throw new Exception("Can't match any driver to open database. Please specify the driver of connection string use property name \"connectionName.driver\"")
        }

        if (Properties.contains(connectionName + ".username")) {
            username = Properties.get(connectionName + ".username")
        }

        if (Properties.contains(connectionName + ".password")) {
            password = Properties.get(connectionName + ".password")
        }

        if (Properties.contains(connectionName + ".overtime")) {
            overtime = Properties.get(connectionName + ".overtime").toInt
        }

        if (Properties.contains(connectionName + ".retry")) {
            retry = Properties.get(connectionName + ".retry").toInt
        }

        new JDBC(dbType, connectionString, driver, username, password, overtime, retry)
    }


    def get(connectionName: String): JDBC = {
        if (!connections.contains(connectionName)) {
            connections += connectionName -> take(connectionName)
        }

        connections(connectionName)
    }
}

class JDBC(var dbType: String,
           var connectionString: String = "",
           var driver: String = "",
           var username: String = "",
           var password: String = "",
           var overtime: Int = 0, //超时时间, 0 不限制
           var retryLimit: Int = 0 //重试次数, 0 不重试
          ) {

    var alternativeDriver: String = ""

    if (driver.contains(",")) {
        driver = driver.substring(0, driver.indexOf(","))
        alternativeDriver = driver.substring(driver.indexOf(",") + 1)
    }

    if (dbType == DBType.Memory) {
        connectionString = "jdbc:sqlite::memory:"
    }
    else if (dbType == DBType.SQLite && !connectionString.startsWith("jdbc:sqlite:")) {
        connectionString = "jdbc:sqlite:" + connectionString
    }

    if (overtime == 0) {
        dbType match {
            case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle =>
                overtime = 10000
            case _ =>
        }
    }
    if (retryLimit == 0) {
        dbType match {
            case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle =>
                retryLimit = 100
            case DBType.Hive | DBType.Spark | DBType.Impala | DBType.Presto =>
                retryLimit = 3
            case _ =>
        }
    }
}
