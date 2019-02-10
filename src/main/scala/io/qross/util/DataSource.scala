package io.qross.util

import java.sql._
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

object DataSource {
    
    val DEFAULT = "mysql.rds"
    val DEFAULT_DATABASE = "zichan360bi_metdata"

    def openDefault() : DataSource = {
        new DataSource(DEFAULT, DEFAULT_DATABASE)
    }

    def createMemoryDatabase: DataSource = {
        new DataSource("sqlite.memory")
    }
    
    
    def queryDataTable(SQL: String, values: Any*): DataTable = {
        val ds: DataSource = new DataSource()
        val dataTable: DataTable = ds.executeDataTable(SQL, values: _*)
        ds.close()
        
        dataTable
    }
    
    def queryDataRow(SQL: String, values: Any*): DataRow = {
        val ds: DataSource = new DataSource()
        val dataRow: DataRow = ds.executeDataRow(SQL, values: _*)
        ds.close()
        
        dataRow
    }
    
    def querySingleValue(SQL: String, values: Any*): Option[String] = {
        val ds: DataSource = new DataSource()
        val value: Option[String] = ds.executeSingleValue(SQL, values: _*)
        ds.close()
        
        value
    }
    
    def queryUpdate(SQL: String, values: Any*): Int = {
        val ds: DataSource = new DataSource()
        val rows: Int = ds.executeNonQuery(SQL, values: _*)
        ds.close()
        
        rows
    }
    
    def queryExists(SQL: String, values: Any*): Boolean = {
        val ds: DataSource = new DataSource()
        val exists = ds.executeExists(SQL, values: _*)
        ds.close()
        
        exists
    }
}

class DataSource (val connectionName: String = DataSource.DEFAULT, var databaseName: String = "") {

    if (connectionName == DataSource.DEFAULT && databaseName == "") {
        databaseName = DataSource.DEFAULT_DATABASE
    }
    
    private val batchSQLs = new ArrayBuffer[String]()
    private val batchValues = new ArrayBuffer[Vector[Any]]()

    private val config = new JDBConfiguration(connectionName)
    private val isMySQL: Boolean = config.driver.contains("mysql")

    private var connection: Option[Connection] = None //current connection
    private var tick: Long = -1L //not opened
    
    def testConnection(): Boolean = {
        var connected = false
        try {
            this.executeResultSet("SELECT 1 FROM dual")
            connected = true
        }
        catch {
            case _: Exception =>
        }

        connected
    }

    
    def open(): Unit = {
        try {
            Class.forName(config.driver).newInstance
            if (config.isAlone) {
                this.connection = Some(DriverManager.getConnection(config.connectionString, config.username, config.password))
            }
            else {
                this.connection = Some(DriverManager.getConnection(config.connectionString))
            }
        } catch {
            case e: InstantiationException => System.err.println("open database InstantiationException " + e.getMessage)
            case e: IllegalAccessException => System.err.println("open database IllegalAccessException " + e.getMessage)
            case e: ClassNotFoundException => System.err.println("open database ClassNotFoundException " + e.getMessage)
            case e: SQLException => System.err.println("open database SQLException " + e.getMessage)
        }

        if (this.isMySQL) {
            this.connection match {
                case Some(conn) =>
                    try {
                        val prest: PreparedStatement = conn.prepareStatement("SELECT 1 FROM dual")
                        val rs = prest.executeQuery()
                        rs.close()
                        prest.close()
                    }
                    catch {
                        case e: Exception =>
                            System.err.println("test connection Exception " + e.getMessage)
                            connection = None
                    }
                case None =>
            }
        }

        if (this.databaseName != "") {
            this.connection match {
                case Some(conn) =>
                    val prest: PreparedStatement = conn.prepareStatement("USE " + this.databaseName)
                    prest.executeUpdate()
                    prest.close()
                case None =>
            }
        }
    }

    
    // ---------- basic command ----------
    
    def executeDataTable(SQL: String, values: Any*): DataTable = {
        
        val table: DataTable = new DataTable
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData

                val columns: Int = meta.getColumnCount
                var fieldName = ""
                for (i <- 1 to columns) {
                    fieldName = meta.getColumnLabel(i) //meta.getColumnName(i) original name
                    // . is illegal char in SQLite and field name contains "." in hive columns
                    if (fieldName.contains(".")) fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1)
                    if (!Pattern.matches("^[a-zA-Z_][a-zA-Z0-9_]*$", fieldName) || table.contains(fieldName)) fieldName = "column" + i
                    //println(meta.getColumnLabel(i) + ": " + meta.getColumnTypeName(i) + ", " + meta.getColumnClassName(i))
                    table.addFieldWithLabel(fieldName, meta.getColumnLabel(i), DataType.ofClassName(meta.getColumnClassName(i))) //meta.getColumnTypeName(i)
                }

                val fields = table.getFieldNames
                while (rs.next) {
                    val row = DataRow()
                    for (i <- 1 to columns) {
                        //print(rs.getObject(i) + ", " + rs.getString(i) + "; ")
                        row.set(fields(i - 1), rs.getString(i))
                    }
                    //println()
                    table.addRow(row)
                }
                if (!connectionName.startsWith("presto.")) {
                    rs.getStatement.close()
                }
                rs.close()

            case None =>
        }
        
        table
    }
    
    def executeDataList(SQL: String, values: Any*): ArrayBuffer[ArrayBuffer[String]] = {
        var list = new ArrayBuffer[ArrayBuffer[String]]
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val columns: Int = rs.getMetaData.getColumnCount
                while (rs.next) {
                    var row = new ArrayBuffer[String]
                    for (i <- 1 to columns) {
                        row += rs.getString(i)
                    }
                    list += row
                }
                if (!connectionName.startsWith("presto.")) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }
        list
    }
    
    def executeDataRow(SQL: String, values: Any*): DataRow = {
        val row: DataRow = new DataRow
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData
                val columns: Int = rs.getMetaData.getColumnCount
                if (rs.next) {
                    for (i <- 1 to columns) {
                        row.set(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    if (!connectionName.startsWith("presto.")) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }
        row
    }
    
    def executeSingleList(SQL: String, values: Any*): List[String] = {
        var list = new ArrayBuffer[String]
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                while (rs.next) {
                    list += rs.getString(1)
                }
                if (!connectionName.startsWith("presto.")) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }
        list.toList
    }
    
    def executeSingleValue(SQL: String, values: Any*): Option[String] = {
        var value: Option[String] = None
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                if (rs.next()) {
                    value = Some(rs.getString(1))
                    if (!connectionName.startsWith("presto.")) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }
        value
    }
    
    def executeExists(SQL: String, values: Any*): Boolean = {
        var result = false
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                if (rs.next()) {
                    result = true
                    if (!connectionName.startsWith("presto.")) {
                        rs.getStatement.close()
                    }
                    rs.close()
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
                    //try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        rs = Some(prest.executeQuery)
                        //prest.close()
                    //} catch {
                    //    case e: SQLException => e.printStackTrace()
                            //if (e.getClass.getSimpleName == "CommunicationsException") {
                            //    Output.writeMessage("MATCHED!")
                            //}
                    //}
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
                    //try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        row = prest.executeUpdate

                        prest.close()
                    //} catch {
                    //    case e: SQLException => e.printStackTrace()
                    //}
                    retry += 1
                }
                row
            case None => -1
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
                    //try {
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

                    //}
                    //catch {
                    //    case e: SQLException => e.printStackTrace()
                    //}
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
                        //try {
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
                        //} catch {
                        //    case e: SQLException => e.printStackTrace()
                        //}
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
            val batchSQL = this.batchSQLs(0)
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
    
    def tableUpdate(SQL: String, table: DataTable): Long = {
        var count = -1
        
        if (table.nonEmpty) {
            if (SQL.contains("?")) {
                this.setBatchCommand(SQL)
                table.foreach(row => {
                    this.addBatch(row.getValues)
                })
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
    
    def tableInsert(SQL: String, table: DataTable): Long = {
        tableUpdate(SQL, table)
    }
    
    def tableDelete(SQL: String, table: DataTable): Long = {
        tableUpdate(SQL, table)
    }
    
    // ---------- storeproceure ----------
    
    // {call storedprocedure(?,?)}
    def processUpdate(storedProcedure: String, values: Any*): Int = {
        var row: Int = -1
        //try {
            val calst: CallableStatement = this.connection.get.prepareCall("{call " + storedProcedure + "}")
            for (i <- 0 until values.length) {
                calst.setObject(i + 1, values(i))
            }
            row = calst.executeUpdate
            calst.close()
        //} catch {
            //case e: SQLException => e.printStackTrace()
        //}
        
        row
    }
    
    def processResult(storedProcedure: String, values: Any*): Unit = {
    
    }
    
    // --------- other ----------
    
    def getIdleTime: Long = {
        this.tick match {
            case -1 => -1L
            case _ => System.currentTimeMillis - this.tick
        }
    }
    
    def openIfNot(): Unit = {
        try {
            var retry = 0
            //idle 10s
            if (this.isMySQL && this.getIdleTime >= 10000) {
                this.close()
            }
            if (this.tick == -1 || this.connection.get.isClosed) {
                while (this.connection.isEmpty && retry < 100) {
                    this.open()
                    if (this.connection.isEmpty) {
                        Timer.sleep(1F)
                        retry += 1
                    }
                }

                if (this.connection.isDefined) {
                    this.tick = System.currentTimeMillis
                }
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        }
    }
    
    def close(): Unit = {
        try {
            if (this.connection.isDefined && !this.connection.get.isClosed) {
                this.connection.get.close()
                this.connection = None
            }
        } catch {
            case e: SQLException => "close database Exception: " + e.printStackTrace()
        }
        this.tick = -1
    }
    
    private def trimSQL(SQL: String): String = {
        var commandText = SQL.trim
        if (commandText.endsWith(";")) {
            commandText = commandText.dropRight(1)
        }
        
        commandText
    }
}