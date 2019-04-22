package io.qross.jdbc

import java.sql._

import io.qross.core.{DataRow, DataTable, DataType}
import io.qross.util.{Parameter, Timer}
import scala.collection.mutable.ArrayBuffer

class JDBC(connectionName: String = "") {
    
    protected var conf: JDBConnection = JDBConnection.get(connectionName)
    
    protected val dbGroup: Int = JDBConnection.group(connectionName, conf.connectionString)
    protected val dbType: String = JDBConnection.recognizeDBType(connectionName, conf.connectionString)
    protected var retryTimes: Int = 0
    protected var retryLimit: Int = dbGroup match {
        case DBGroup.BASE => 0
        case DBGroup.DWS => 3
        case DBGroup.DBS => 0
        case DBGroup.RDBMS => 100
    }
    protected var tick: Long = -1L //not opened
    protected var overtime: Long =  //ms
        if (dbGroup == DBGroup.RDBMS) {
            10000L
        }
        else {
            0
        }
    
    protected var connection: Option[Connection] = None //current connection
    protected var connected: Boolean = false
    
    //for batch update
    protected var batchSQLs = new ArrayBuffer[String]()
    protected var batchValues = new ArrayBuffer[Vector[Any]]()
    
    def retry(retryLimit: Int): JDBC = {
        this.retryLimit = retryLimit
        this
    }
    
    def expire(seconds: Int): JDBC = {
        this.overtime = seconds * 1000L
        this
    }
    
    protected def connect(): Unit = {
        try {
            Class.forName(conf.driver).newInstance
            if (conf.userName == "" && conf.password == "") {
                this.connection = Some(DriverManager.getConnection(conf.connectionString))
            }
            else {
                this.connection = Some(DriverManager.getConnection(conf.connectionString, conf.userName, conf.password))
            }
        } catch {
            case e: InstantiationException => System.err.println("Open database InstantiationException " + e.getMessage)
            case e: IllegalAccessException => System.err.println("Open database IllegalAccessException " + e.getMessage)
            case e: ClassNotFoundException => System.err.println("Open database ClassNotFoundException " + e.getMessage)
            case e: SQLException => System.err.println("Open database SQLException " + e.getMessage)
        }
    }
    
    def open(): Unit = {
        
        //do connect
        this.connect()
        
        while (this.connection.isEmpty && retryTimes < retryLimit) {
            Timer.sleep(1F)
            
            //reconnect
            this.connect()
            retryTimes += 1
        }
    
        if (connection.nonEmpty) {
            retryTimes = 0
            connected = true
            this.tick = System.currentTimeMillis()
        }
        else {
            throw new SQLException(s"Connection times reach upper limit $retryLimit.")
        }
    }
    
    // ---------- basic command ----------
    
    def executeDataTable(SQL: String, values: Any*): DataTable = {

        val table: DataTable = new DataTable
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    val meta: ResultSetMetaData = rs.getMetaData
                    val columns: Int = meta.getColumnCount
                    for (i <- 1 to columns) {
                        table.addField(meta.getColumnLabel(i), DataType.ofClassName(meta.getColumnClassName(i))) //meta.getColumnTypeName(i)
                    }
                    
                    val fields = table.getFieldNames
                    while (rs.next) {
                        val row = DataRow()
                        for (i <- 1 to columns) {
                            row.set(fields(i - 1), rs.getObject(i))
                        }
                        table.addRow(row)
                    }
                    rs.getStatement.close()
                    rs.close()
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        
        table
    }
    
    def executeDataList(SQL: String, values: Any*): ArrayBuffer[ArrayBuffer[String]] = {
        var list = new ArrayBuffer[ArrayBuffer[String]]
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    val columns: Int = rs.getMetaData.getColumnCount
                    while (rs.next) {
                        var row = new ArrayBuffer[String]
                        for (i <- 1 to columns) {
                            row += rs.getString(i)
                        }
                        list += row
                    }
                    rs.getStatement.close()
                    rs.close()
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        list
    }
    
    def executeDataRow(SQL: String, values: Any*): DataRow = {
        val row: DataRow = new DataRow
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    val meta: ResultSetMetaData = rs.getMetaData
                    val columns: Int = rs.getMetaData.getColumnCount
                    if (rs.next) {
                        for (i <- 1 to columns) {
                            row.set(meta.getColumnLabel(i), rs.getObject(i))
                        }
                        rs.getStatement.close()
                        rs.close()
                    }
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        row
    }
    
    def executeSingleList(SQL: String, values: Any*): List[String] = {
        var list = new ArrayBuffer[String]
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    while (rs.next) {
                        list += rs.getString(1)
                    }
                    rs.getStatement.close()
                    rs.close()
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        list.toList
    }
    
    def executeSingleValue(SQL: String, values: Any*): Option[String] = {
        var value: Option[String] = None
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    if (rs.next()) {
                        value = Some(rs.getString(1))
                        rs.getStatement.close()
                        rs.close()
                    }
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        value
    }
    
    def executeExists(SQL: String, values: Any*): Boolean = {
        var result = false
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                try {
                    if (rs.next()) {
                        result = true
                        rs.getStatement.close()
                        rs.close()
                    }
                } catch {
                    case e: SQLException => e.printStackTrace()
                }
            case None =>
        }
        
        result
    }
    
    def executeResultSet(SQL: String, values: Any*): Option[ResultSet] = {
        this.openIfNot()
        
        this.connection match {
            case Some(conn) =>
                var rs: Option[ResultSet] = None
                var retry: Int = 0
                while (rs.isEmpty && retry < 3) {
                    try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        rs = Some(prest.executeQuery)
                        //prest.close()
                    } catch {
                        case e: SQLException => e.printStackTrace()
                        //if (e.getClass.getSimpleName == "CommunicationsException") {
                        //    Output.writeMessage("MATCHED!")
                        //}
                    }
                    retry += 1
                }
                rs
            case None => None
        }
    }
    
    def executeNonQuery(SQL: String, values: Any*): Int = {
        this.openIfNot()
        
        this.connection match {
            case Some(conn) =>
                var row: Int = -1
                var retry: Int = 0
                while(row == -1 && retry < 3) {
                    try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        row = prest.executeUpdate
                        prest.close()
                    } catch {
                        case e: SQLException => e.printStackTrace()
                    }
                    retry += 1
                }
                row
            case None => -1
        }
    }
    
    protected def openIfNot(): Unit = {
        if (this.connected && (this.overtime > 0 && System.currentTimeMillis - this.tick > this.overtime)) {
            this.close()
        }
        if (!this.connected || this.connection.isEmpty || this.connection.get.isClosed) {
            this.open()
        }
    }
    
    // ---------- batch update ----------
    
    def addBatchCommand(SQL: String): Unit = {
        this.batchSQLs += trimSQL(SQL)
    }
    
    def executeBatchCommands(): Int = {
        this.openIfNot()
        
        this.connection match {
            case Some(conn) =>
                var count = 0
                if (this.batchSQLs.nonEmpty) {
                    try {
                        conn.setAutoCommit(false)
                        val stmt = conn.createStatement()
                        //val prest: PreparedStatement = conn.prepareStatement("")
                        this.batchSQLs.foreach(SQL => {
                            stmt.addBatch(SQL)
                            count += 1
                        })
                        stmt.executeBatch()
                        conn.commit()
                        conn.setAutoCommit(true)
                        stmt.clearBatch()
                        stmt.close()
                    }
                    catch {
                        case e: SQLException => e.printStackTrace()
                    }
                    this.batchSQLs.clear()
                }
                
                count
            case None => 0
        }
    }
    
    def setBatchCommand(SQL: String): Unit = {
        if (this.batchSQLs.nonEmpty) {
            this.batchSQLs.clear()
        }
        this.batchSQLs += trimSQL(SQL)
    }
    
    def addBatch(values: Any*): Unit = {
        this.batchValues += values.toVector
    }
    
    def addBatch(values: List[Any]): Unit = {
        this.batchValues += values.toVector
    }
    
    def addBatch(values: Vector[Any]): Unit = {
        this.batchValues += values
    }
    
    def executeBatchUpdate(commitOnExecute: Boolean = true): Int = {
        this.openIfNot()
        
        this.connection match {
            case Some(conn) =>
                var count: Int = 0
                if (this.batchSQLs.nonEmpty) {
                    if (this.batchValues.nonEmpty) {
                        try {
                            conn.setAutoCommit(false)
                            val prest: PreparedStatement = conn.prepareStatement(this.batchSQLs(0))
                            for (values <- this.batchValues) {
                                for (i <- values.indices) {
                                    prest.setObject(i + 1, values(i))
                                }
                                prest.addBatch()
                                
                                count += 1
                                if (count % 1000 == 0) {
                                    prest.executeBatch
                                    if (commitOnExecute) {
                                        conn.commit()
                                    }
                                }
                            }
                            if (count % 1000 > 0) {
                                prest.executeBatch
                            }
                            conn.commit()
                            conn.setAutoCommit(true)
                            prest.clearBatch()
                            prest.close()
                        } catch {
                            case e: SQLException => e.printStackTrace()
                        }
                        this.batchValues.clear()
                    }
                    this.batchSQLs.clear()
                }
                
                count
            
            case None => 0
        }
    }
    
    def executeBatchInsert(batchSize: Int = 1000): Int = {
        
        var count: Int = 0
        if (this.batchSQLs.nonEmpty && this.batchValues.nonEmpty) {
            var location: Int = 0
            var batchSQL = this.batchSQLs(0)
            var baseSQL: String = batchSQL.toUpperCase
            if (baseSQL.contains("VALUES")) {
                location = baseSQL.indexOf("VALUES") + 6
                baseSQL = batchSQL.substring(0, location) + " "
            }
            else {
                baseSQL = batchSQL + " VALUES "
            }
            
            var rows = new ArrayBuffer[String]
            var v: String = ""
            var vs: String = ""
            for (values <- this.batchValues) {
                vs = "('"
                for (i <- values.indices) {
                    v = values(i).toString
                    if (i > 0) {
                        vs += "', '"
                    }
                    if (v.contains("'")) {
                        v = v.replace("'", "''")
                    }
                    vs += v
                }
                vs += "')"
                rows += vs
                
                if (rows.size >= batchSize) {
                    count += this.executeNonQuery(baseSQL + rows.mkString(","))
                    rows.clear()
                }
            }
            if (rows.nonEmpty) {
                count += this.executeNonQuery(baseSQL + rows.mkString(","))
                rows.clear()
            }
            this.batchValues.clear()
            this.batchSQLs.clear()
        }
        
        count
    }
    
    def tableSelect(SQL: String, table: DataTable): DataTable = {
        val result = DataTable()
        
        if (SQL.contains("?")) {
            table.foreach(row => {
                result.merge(this.executeDataTable(SQL, row.getValues: _*))
            })
        }
        else {
            val param = Parameter(SQL)
            if (param.matched) {
                table.foreach(row => {
                    result.merge(this.executeDataTable(param.replaceWith(row)))
                })
            }
            else {
                result.merge(this.executeDataTable(SQL))
            }
        }
        
        result
    }
    
    def tableUpdate(SQL: String, table: DataTable): Int = {
        var count = -1
        
        if (table.nonEmpty) {
            if (SQL.contains("?")) {
                this.setBatchCommand(SQL)
                table.foreach(row => this.addBatch(row.getValues))
                count = this.executeBatchUpdate()
            }
            else {
                val param = Parameter(SQL)
                if (param.matched) {
                    table.foreach(row => {
                        this.addBatchCommand(param.replaceWith(row))
                    })
                    count = this.executeBatchCommands()
                }
                else {
                    count = this.executeNonQuery(SQL)
                }
            }
        }
        
        count
    }
    
    def tableInsert(SQL: String, table: DataTable): Int = {
        tableUpdate(SQL, table)
    }
    
    def tableDelete(SQL: String, table: DataTable): Int = {
        tableUpdate(SQL, table)
    }
    
    def close(): Unit = {
        this.connection match {
            case Some(conn) => if (!conn.isClosed) conn.close()
            case _ =>
        }
        connected = false
    
        this.tick = -1
    }
    
    protected def trimSQL(SQL: String): String = {
        var commandText = SQL.trim
        if (commandText.endsWith(";")) {
            commandText = commandText.dropRight(1)
        }
        
        commandText
    }
    
    // {call storedprocedure(?,?)}
    /*
    def processUpdate(storedProcedure: String, values: Any*): Int = {
        var row: Int = -1
        try {
            val calst: CallableStatement = this.connection.get.prepareCall("{call " + storedProcedure + "}")
            for (i <- 0 until values.length) {
                calst.setObject(i + 1, values(i))
            }
            row = calst.executeUpdate
            calst.close()
        } catch {
            case e: SQLException => e.printStackTrace()
        }
        
        row
    }
    
    def processResult(storedProcedure: String, values: Any*): Unit = {
    
    }
    */
}
