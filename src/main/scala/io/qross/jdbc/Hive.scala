package io.qross.jdbc

class Hive(connectionName: String) extends JDBC(connectionName: String) {
    override protected val dbGroup: Int = DBGroup.DWS
    override protected val dbType: String = DBType.Hive
    override protected var retryLimit: Int = 3
    override protected var overtime: Long = 0L //ms
}