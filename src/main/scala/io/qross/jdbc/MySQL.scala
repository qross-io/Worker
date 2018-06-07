package io.qross.jdbc

object MySQL {
    def openPrimary(): MySQL = {
        new MySQL(JDBConnection.PRIMARY)
    }
    
    def testConnection(): Boolean = {
        val ds = new MySQL(JDBConnection.PRIMARY).retry(0)
        ds.open()
        val connected = ds.connection != null
        ds.close()
        connected
    }
}

class MySQL(connectionName: String) extends JDBC(connectionName: String) {
    
    override protected val dbGroup: Int = DBGroup.RDBMS
    override protected val dbType: String = DBType.MySQL
    override protected var retryLimit: Int = 100
    override protected var overtime: Long = 12000L //ms
    
}
