package io.qross.jdbc

import io.qross.util.FilePath

object SQLite {
    
    val CACHE = "sqlite.memory"
    
    def connectMemoryDatabase: SQLite = {
        new SQLite(CACHE)
    }
}

class SQLite(fileNameOrFullPath: String = SQLite.CACHE) extends JDBC {
    
    override protected val conf: JDBConnection =
        if (JDBConnection.contains(fileNameOrFullPath)) {
            JDBConnection.get(fileNameOrFullPath)
        }
        else {
            new JDBConnection(DBType.SQLite, "jdbc:sqlite:" + FilePath.locate(fileNameOrFullPath))
        }
    
    override protected val dbGroup: Int = DBGroup.DBS
    override protected val dbType: String = DBType.SQLite
    override protected var retryLimit: Int = 0
    override protected var overtime: Long = 0L //ms
}
