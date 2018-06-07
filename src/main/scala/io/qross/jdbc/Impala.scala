package io.qross.jdbc

class Impala(connectionName: String) extends JDBC(connectionName: String) {
    override protected val dbGroup: Int = DBGroup.DWS
    override protected val dbType: String = DBType.Impala
}