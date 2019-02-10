package io.qross.util

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.util.DataType.DataType
import io.qross.util.Output._
import org.apache.tools.ant.types.FileList.FileName

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray

class DataHub () {
    
    private val SOURCES = mutable.HashMap[String, DataSource]()

    //temp database
    private val holder = s"temp_${DateTime.now.getString("yyyyMMddHHmmssSSS")}_${Math.round(Math.random() * 10000000D)}.sqlite"

    private var CURRENT: DataSource = _   //current dataSource - open
    private var TARGET: DataSource = _    //current dataDestination - saveAs

    private var DEBUG = true
    
    private val TABLE = DataTable()  //current buffer
    private val BUFFER = new mutable.HashMap[String, DataTable]() //all buffer

    private var TO_BE_CLEAR: Boolean = false

    //selectSQL, @param
    private val pageSQLs = new mutable.HashMap[String, String]()
    //selectSQL, (@param, beginKey, endKey, blockSize)
    private val blockSQLs = new mutable.HashMap[String, (String, Long, Long, Int)]()
    //存储生产者消费者处理模式中的中间结果
    private val DATA = new ConcurrentLinkedQueue[DataTable]()

    private var EXCEL: Excel = _
    private var EMAIL: Email = _

    private var JSON: Json = _
    private var READER: FileReader = _
    private val ZIP = new Zip()

    //全局变量-最后一次get方法的结果集数量
    private var $COUNT: Int = 0
    private var $TOTAL: Int = 0
    
    // ---------- system ----------
    
    def debug(enabled: Boolean = true): DataHub = {
        DEBUG = enabled
        this
    }

    def interruptOnException(enabled: Boolean = true) : DataHub = {
        //INTERRUPT_ON_EXCEPTION = enabled
        this
    }
    
    // ---------- open ----------
    
    def openCache(): DataHub = {
        reset()
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        CURRENT = SOURCES("CACHE")
        this
    }

    def openTemp(): DataHub = {
        reset()
        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(holder)
        }
        CURRENT = SOURCES("TEMP")
        this
    }
    
    def openDefault(): DataHub = {
        reset()
        if (!SOURCES.contains("DEFAULT")) {
            SOURCES += "DEFAULT" -> DataSource.openDefault()
        }
        CURRENT = SOURCES("DEFAULT")
        this
    }
    
    def open(connectionName: String, database: String = ""): DataHub = {
        reset()
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName, database)
        }
        CURRENT = SOURCES(connectionName)
        this
    }

    def openRDS(): DataHub = {
        open("mysql.rds")
    }

    def openRDS(databaseName: String): DataHub = {
        open("mysql.rds", databaseName)
    }

    def openODS(): DataHub = {
        open("mysql.ods")
    }

    def openST(): DataHub = {
        open("mysql.rds", "zichan360bi_st")
    }

    def openMID(): DataHub = {
        open("mysql.rds", "zichan360bi_mid")
    }

    def openDW(): DataHub = {
        open("mysql.rds", "zichan360bi_dw")
    }

    def openSlave(): DataHub = {
        open("mysql.slave")
    }

    def openSlave(databaseName: String): DataHub = {
        open("mysql.slave", databaseName)
    }

    def openAuthCenter(): DataHub = {
        open("mysql.slave", "auth_center")
    }

    def openCallCenter(): DataHub = {
        open("mysql.slave", "callcenter")
    }

    def openCase(): DataHub = {
        open("mysql.slave", "zichan360_case")
    }

    def openHive(): DataHub = {
        open("hive.default")
    }

    def openLocalHive(): DataHub = {
        open("hive.localhost")
    }

    def openPresto(): DataHub = {
        open("presto.default")
    }

    //文本文件相关

    def openTextFile(path: String): DataHub = {
        READER = FileReader(path)
        this
    }

    def useDelimiter(delimiter: String): DataHub = {
        READER.delimit(delimiter)
        this
    }

    def asTable(tableName: String): DataHub = {
        READER.asTable(tableName)
        this
    }

    def withColumns(fields: String*): DataHub = {
        READER.withColumns(fields: _*)
        this
    }

    def etl(handler: DataTable => DataTable): DataHub = {
        READER.etl(handler)
        this
    }

    /*
    openTextFile("")
        .asTable("")
        .withColumns("")
        .etl(handler)
        .page("SELECT * FROM tableName LIMIT @offset, 10000")

        .get("")
        .page("")
        .block()
    .saveAs("")
        .put("")
     */

    def readAllAsTable(fields: String*): DataHub = {
        TABLE.cut(READER.readAllAsTable(fields: _*))
        this
    }
    
    // ---------- save as ----------
    
    def saveAsCache(): DataHub = {
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        TARGET = SOURCES("CACHE")
        this
    }

    def saveAsTemp(): DataHub = {
        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(holder)
        }
        TARGET = SOURCES("TEMP")
        this
    }

    def saveAsDefault(): DataHub = {
        if (!SOURCES.contains("DEFAULT")) {
            SOURCES += "DEFAULT" -> DataSource.openDefault()
        }
        TARGET = SOURCES("DEFAULT")
        this
    }
    
    def saveAs(connectionName: String, database: String = ""): DataHub = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName, database)
        }
        TARGET = SOURCES(connectionName)
        this
    }

    def saveAsRDS(database: String): DataHub = {
        saveAs("mysql.rds", database)
    }

    def saveAsODS(): DataHub = {
        saveAs("mysql.rds", "zichan360bi_ods")
    }

    def saveAsST(): DataHub = {
        saveAs("mysql.rds", "zichan360bi_st")
    }

    def saveAsMID(): DataHub = {
        saveAs("mysql.rds", "zichan360bi_mid")
    }

    def saveAsDW(): DataHub = {
        saveAs("mysql.rds", "zichan360bi_dw")
    }

    def saveAsNewTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
        val path = FilePath.locate(fileNameOrFullPath)
        FileWriter(path, true).delimit(delimiter).writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }
    
    def saveAsTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
        val path = FilePath.locate(fileNameOrFullPath)
        FileWriter(path, false).delimit(delimiter).writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def saveAsNewCsvFile(fileNameOrFullPath: String): DataHub = {
        val path = FilePath.locate(fileNameOrFullPath)
        FileWriter(path, true).delimit(",").writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
        val path = FilePath.locate(fileNameOrFullPath)
        FileWriter(path, false).delimit(",").writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def writeEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        this
    }

    def fromResourceTemplate(resourceFile: String): DataHub = {
        EMAIL.fromTemplate(resourceFile)
        this
    }

    def withDefaultSignature(): DataHub = {
        EMAIL.withDefaultSignature()
        this
    }

    def withSignature(resourceFile: String): DataHub = {
        EMAIL.withSignature(resourceFile)
        this
    }

    def setEmailContent(content: String): DataHub = {
        EMAIL.setContent(content)
        this
    }

    def placeEmailTitle(title: String): DataHub = {
        EMAIL.placeContent("${title}", title)
        this
    }

    def placeEmailContent(content: String): DataHub = {
        EMAIL.placeContent("${content}", content)
        this
    }

    def placeEmailDataTable(placeHolder: String = ""): DataHub = {
        if (placeHolder == "") {
            EMAIL.placeContent("${data}", TABLE.toHtmlString)
        }
        else {
            EMAIL.placeContent("${" + placeHolder + "}", TABLE.toHtmlString)
        }

        TO_BE_CLEAR = true

        this
    }

    def placeEmailHolder(replacements: (String, String)*): DataHub = {
        for ((placeHolder, replacement) <- replacements) {
            EMAIL.placeContent(placeHolder, replacement)
        }
        this
    }

    def placeEmailContentWithRow(i: Int = 0): DataHub = {
        TABLE.getRow(i) match {
            case Some(row) => EMAIL.placeContentWidthDataRow(row)
            case None =>
        }

        TO_BE_CLEAR = true
        this
    }

    def placeEmailContentWithFirstRow(): DataHub = {
        TABLE.firstRow match {
            case Some(row) => EMAIL.placeContentWidthDataRow(row)
            case _ =>
        }

        TO_BE_CLEAR = true

        this
    }

    def to(receivers: String): DataHub = {
        EMAIL.to(receivers)
        this
    }

    def cc(receivers: String): DataHub = {
        EMAIL.cc(receivers)
        this
    }

    def bcc(receivers: String): DataHub = {
        EMAIL.bcc(receivers)
        this
    }

    def attach(path: String): DataHub = {
        EMAIL.attach(path)
        this
    }

    def send(): DataHub = {
        EMAIL.placeContent("${title}", "")
        EMAIL.placeContent("${content}", "")
        EMAIL.placeContent("${data}", "")
        EMAIL.send()
        this
    }

    def saveAsExcel(fileNameOrPath: String): DataHub = {
        EXCEL = new Excel(fileNameOrPath)
        ZIP.addFile(EXCEL.path)
        this
    }

    def saveAsNewExcel(fileNameOrPath: String): DataHub = {
        deleteFile(fileNameOrPath)
        EXCEL = new Excel(fileNameOrPath)
        ZIP.addFile(EXCEL.path)
        this
    }

    def fromExcelTemplate(templateName: String): DataHub = {
        EXCEL.fromTemplate(templateName)
        this
    }

    def appendSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withoutHeader()
                .appendTable(TABLE)
        }


        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withoutHeader()
                    .appendTable(table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def appendSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withHeader()
                .appendTable(TABLE)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withHeader()
                    .appendTable(table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def writeCellValue(value: Any, sheetName: String = "sheet1", rowIndex: Int = 0, colIndex: Int = 0): DataHub = {
        EXCEL.openSheet(sheetName).setCellValue(value, rowIndex, colIndex).setInitialRow(rowIndex).setInitialColumn(colIndex)
        this
    }

    def writeSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
          .setInitialRow(initialRow)
          .setInitialColumn(0)
          .openSheet(sheetName)
          .withoutHeader()
          .writeTable(TABLE)

        TO_BE_CLEAR = true

        this
    }

    def writeSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
          .setInitialRow(initialRow)
          .setInitialColumn(0)
          .openSheet(sheetName)
          .withHeader()
          .writeTable(TABLE)

        TO_BE_CLEAR = true

        this
    }

    def setRegionStyle(rows: (Int, Int), cols: (Int, Int), styles: (String, Any)*): DataHub = {
        EXCEL.setStyle(rows, cols, 1, styles: _*)
        this
    }

    def withCellStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setCellStyle(EXCEL.getInitialRow, EXCEL.getInitialColumn, styles: _*)
        this
    }

    def withHeaderStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowStyle(
            EXCEL.getInitialRow,
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withAlternateRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setAlternateRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*
        )
        this
    }

    def mergeCells(rows: (Int, Int), cols: (Int, Int)): DataHub = {
        EXCEL.mergeRegion(rows, cols)
        this
    }

    def removeMergeRegion(firstRow: Int, firstColumn: Int): DataHub = {
        EXCEL.removeMergedRegion(firstRow, firstColumn)
        this
    }

    //actualInitialRow - append
    //def withStyle - initialRow + 1, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withAlternateStyle - initialRow + 2, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withHeaderStyle - initialRow, initialRow, initialColumn, , TABLE.columns.size
    //def mergeCells

    def attachExcelToEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        EMAIL.attach(EXCEL.fileName)
        this
    }

    def deleteFile(fileName: String): DataHub = {
        new File(FilePath.locate(fileName)).delete()
        this
    }

    def andZip(fileNameOrFullPath: String): DataHub = {
        ZIP.compress(FilePath.locate(fileNameOrFullPath))
        this
    }

    def andZipAll(fileNameOrFullPath: String): DataHub = {
        ZIP.compressAll(FilePath.locate(fileNameOrFullPath))
        this
    }

    def addFileToZipList(fileNameOrFullPath: String): DataHub = {
        ZIP.addFile(FilePath.locate(fileNameOrFullPath))
        this
    }

    //清除列表并删除文件
    def andClear(): DataHub = {
        ZIP.deleteAll()
        this
    }

    //仅清除列表
    def resetZipList(): DataHub = {
        ZIP.clear()
        this
    }

    def showZipList(): DataHub = {
        ZIP.zipList.foreach(file => println(file.getAbsolutePath))
        this
    }

    def attachZipToEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        EMAIL.attach(ZIP.zipFile)
        this
    }

    // ---------- Reset ----------

    def reset(): DataHub = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            pageSQLs.clear()
            $TOTAL = 0
            TO_BE_CLEAR = false
        }
        this
    }
    
    // ---------- cache ----------
    
    def cache(tableName: String, primaryKey: String = ""): DataHub = {

        if (TABLE.nonEmptySchema) {
            cache(tableName, TABLE, primaryKey)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                cache(tableName, table, primaryKey)
                table.clear()
            })
        }

        this
    }
    
    def cache(tableName: String, table: DataTable, primaryKey: String): DataHub = {

        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }

        //var createSQL = "" + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "") {
            createSQL += s" $primaryKey INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getFields) {
            fields += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        SOURCES("CACHE").executeNonQuery(createSQL)
        fields.clear()
    
        if (DEBUG) {
            table.show(10)
            writeMessage(createSQL)
        }

        if (table.nonEmpty) {
            SOURCES("CACHE").tableUpdate("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()
        
        TO_BE_CLEAR = true
        
        this
    }

    def debugger(info: String = ""): DataHub = {
        println(info)
        println("pageSQLs")
        pageSQLs.foreach(println)
        println("blockSQLs")
        blockSQLs.foreach(println)

        this
    }

    // ---------- temp ----------

    def temp(tableName: String, keys: String*): DataHub = {

        if (TABLE.nonEmptySchema) {
            temp(tableName, TABLE, keys: _*)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                temp(tableName, table, keys: _*)
                table.clear()
            })
        }

        this
    }

    def temp(tableName: String, table: DataTable, keys: String*): DataHub = {

        /*
        keys format
        [PRIMARY [KEY]] keyName - if not exists in table, will add as primary key with auto increment - will be ignored if exists
        [KEY] keyName[,keyName2,...] - common index, fields must exists
        UNIQUE [KEY] keyName[,keyName2,...] - unique index, fields must exists
        */
        var primaryKey = ""
        val indexSQL = s"CREATE #{unique}INDEX IF NOT EXISTS idx_${tableName}_#{fields} ON $tableName (#{keys})"
        val indexes = new mutable.ArrayBuffer[String]()
        keys.foreach(key => {
            var index = key.trim()
            var unique = ""

            if (index.toUpperCase().startsWith("PRIMARY ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
            }
            if (index.toUpperCase().startsWith("UNIQUE ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
                unique = "UNIQUE "
            }
            if (index.toUpperCase().startsWith("KEY ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
            }
            index = index.replace(" ", "")

            if (!index.contains(",") && !table.contains(index)) {
                primaryKey = index
            }
            else {
                indexes += indexSQL.replace("#{unique}", unique)
                    .replace("#{fields}", index.replace(",", "_"))
                    .replace("#{keys}", index)
            }
        })

        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(FilePath.locate(holder))
        }

        //var createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "") {
            createSQL += s" $primaryKey INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getFields) {
            fields += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        SOURCES("TEMP").executeNonQuery(createSQL)
        fields.clear()

        if (DEBUG) {
            table.show(10)
            writeMessage(createSQL)
        }

        if (table.nonEmpty) {
            SOURCES("TEMP").tableUpdate("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()

        if (indexes.nonEmpty) {
            indexes.foreach(SQL => {
                println(SQL)
                SOURCES("TEMP").executeNonQuery(SQL)
            })
        }

        TO_BE_CLEAR = true

        this
    }

    // ---------- base method ----------

    //execute SQL on target dataSource
    def set(nonQuerySQL: String, values: Any*): DataHub = {
        if (DEBUG) {
              println(nonQuerySQL)
          }

        CURRENT.executeNonQuery(nonQuerySQL, values: _*)
        this
    }

    //keyword table name must add tow type quote like "`case`" in presto
    def select(selectSQL: String, values: Any*): DataHub = get(selectSQL, values: _*)
    def get(selectSQL: String, values: Any*): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        //TABLE.merge(CURRENT.executeDataTable(selectSQL, values: _*))
        PlaceHolder("@").findIn(selectSQL) match {
            case Some(param) => pageSQLs += selectSQL -> param
            case None =>
                TABLE.merge(CURRENT.executeDataTable(selectSQL, values: _*))
                $TOTAL += TABLE.count()
                $COUNT = TABLE.count()
        }

        this
    }

    def COUNT: Int = {
        $COUNT
    }

    def TOTAL: Int = {
        $TOTAL
    }

    //按limit分页
    //select * from table limit @offset, 10000  多线程
    //select * from table where id>@id LIMIT 10000  单线程
    def page(selectSQL: String): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        PlaceHolder("@").findIn(selectSQL) match {
            case Some(param) => pageSQLs += selectSQL -> param
            case None => TABLE.merge(CURRENT.executeDataTable(selectSQL))
        }

        this
    }

    //按主键分块读取
    //begin_id, end_id
    //每10000行一块

    //select * from table where id>@id AND id<=@id
    //select id from table order by id asc limit 1
    //select max(id) from table
    def block(selectSQL: String, beginKeyOrSQL: Any, endKeyOrSQL: Any, blockSize: Int = 10000): DataHub = {

        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        PlaceHolder("@").findIn(selectSQL) match {
            case Some(param) => blockSQLs +=
                selectSQL -> (param,
                             if (beginKeyOrSQL.isInstanceOf[String]) CURRENT.executeDataRow(beginKeyOrSQL.asInstanceOf[String]).getFirstLong() else beginKeyOrSQL.asInstanceOf[Long],
                             if (endKeyOrSQL.isInstanceOf[String]) CURRENT.executeDataRow(endKeyOrSQL.asInstanceOf[String]).getFirstLong() else endKeyOrSQL.asInstanceOf[Long],
                             blockSize)
            case None => TABLE.merge(CURRENT.executeDataTable(selectSQL))
        }

        this
    }

    def join(selectSQL: String, on: (String, String)*): DataHub = {
        TABLE.join(CURRENT.executeDataTable(selectSQL), on: _*)
        this
    }
    
    //execute SQL on target dataSource
    def fit(nonQuerySQL: String, values: Any*): DataHub = {
        TARGET.executeNonQuery(nonQuerySQL, values: _*)
        this
    }
   
    def insert(insertSQL: String): DataHub = put(insertSQL)
    def update(updateSQL: String): DataHub = put(updateSQL)
    def delete(deleteSQL: String): DataHub = put(deleteSQL)
    def put(nonQuerySQL: String): DataHub = {

        if (TABLE.nonEmpty) {
            if (DEBUG) {
                TABLE.show(10)
                println(nonQuerySQL)
            }
            TARGET.tableUpdate(nonQuerySQL, TABLE)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                TARGET.tableUpdate(nonQuerySQL, table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def insert(insertSQL: String, table: DataTable): DataHub = put(insertSQL, table)
    def update(updateSQL: String, table: DataTable): DataHub = put(updateSQL, table)
    def delete(deleteSQL: String, table: DataTable): DataHub = put(deleteSQL, table)
    def put(nonQuerySentence: String, table: DataTable): DataHub = {
        TARGET.tableUpdate(nonQuerySentence, table)
        this
    }

    //dataSource: target dataSource
    private def stream(handler: DataTable => Unit): Unit = {

        for ((selectSQL, param) <- pageSQLs) {
            if (param.equalsIgnoreCase("offset")) {
                //offset
                val pageSize = "\\d+".r.findFirstMatchIn(selectSQL.substring(selectSQL.indexOf("@" + param) + 7)) match {
                    case Some(ps) => ps.group(0).toInt
                    case None => 10000
                }

                val cube = new Cube()
                val parallel = new Parallel()
                //producer
                for (i <- 0 until 8) {  //Global.CORES
                    parallel.add(new Mover(CURRENT, cube, selectSQL, "@" + param, pageSize))
                }
                parallel.startAll(1)
                //consumer
                while(!cube.isClosed || Mover.DATA.size() > 0 || parallel.running) {
                    val table = Mover.DATA.poll()
                    if (table != null) {
                        $TOTAL += table.count()
                        $COUNT = table.count()
                        handler(table)
                        Output.writeMessage(s"$pageSize SAVED")
                    }
                    Timer.sleep(0.1F)
                }
                Output.writeMessage("Exit While")
                parallel.waitAll()
                Output.writeMessage("Exit All Page")

                /* origin code for single thread
                var page = 0
                var continue = true
                do {
                    val table = CURRENT.executeDataTable(selectSQL.replace("@" + param, String.valueOf(page * pageSize)))
                    if (table.nonEmpty) {
                        handler(table)
                        //dataSource.tableUpdate(nonQuerySQL, table)
                        page += 1
                    }
                    else {
                        continue = false
                    }
                }
                while(continue)
                */
            }
            else {
                //SELECT * FROM table WHERE id>@id LIMIT 10000;

                var id = 0L //primaryKey
                var continue = true
                do {
                    val table = CURRENT.executeDataTable(selectSQL.replace("@" + param, String.valueOf(id)))
                    $TOTAL += table.count()
                    $COUNT = table.count()
                    if (table.nonEmpty) {
                        //dataSource.tableUpdate(nonQuerySQL, table)
                        if (table.contains(param)) {
                            id = table.max(param).firstCellLongValue
                        }
                        else {
                            throw new Exception("Result set must contains primiary key: " + param)
                        }

                        handler(table)
                    }
                    else {
                        continue = false
                    }
                    table.clear()
                }
                while (continue)
            }
        }

        for ((selectSQL, config) <- blockSQLs) {
            //id

            //SELECT * FROM table WHERE id>@id AND id<=@id;

            //producer
            var i = config._2 - 1
            while (i < config._3) {
                var SQL = selectSQL
                SQL = s"@${config._1}".r.replaceFirstIn(SQL, i.toString)
                i += config._4
                if (i > config._3) {
                    i = config._3
                }
                SQL = s"@${config._1}".r.replaceFirstIn(SQL, i.toString)
                println(SQL)
                Blocker.QUEUE.add(SQL)
            }

            //consumer
            val parallel = new Parallel()
            //producer
            for (i <- 0 until 8) {  //Global.CORES
                parallel.add(new Blocker(CURRENT))
            }
            parallel.startAll(1)
            //consumer
            while(Blocker.DATA.size() > 0 || parallel.running) {
                val table = Blocker.DATA.poll()
                if (table != null) {
                    $TOTAL += table.count()
                    $COUNT = table.count()
                    handler(table)
                    Output.writeMessage(s"${config._4} SAVED")
                }
                Timer.sleep(0.1F)
            }
            Output.writeMessage("Exit Block While")
            parallel.waitAll()
            Output.writeMessage("Exit All Block")
        }
    }
    
    // ---------- buffer basic ----------
    
    //switch table
    def from(tableName: String): DataHub = {
        TABLE.clear()
        
        if (BUFFER.contains(tableName)) {
            TABLE.union(BUFFER(tableName))
        }
        else {
            throw new Exception(s"There is no table named $tableName in buffer.")
        }
        this
    }
    
    def buffer(tableName: String, table: DataTable): DataHub = {
        BUFFER += tableName -> table
        TABLE.copy(table)
        this
    }
    
    def buffer(table: DataTable): DataHub = {
        TABLE.copy(table)
        this
    }
    
    def buffer(tableName: String): DataHub = {
        BUFFER += tableName -> DataTable.from(TABLE)
        this
    }
    
    def merge(table: DataTable): DataHub = {
        TABLE.merge(table)
        this
    }
    
    def merge(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).merge(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def union(table: DataTable): DataHub = {
        TABLE.union(table)
        this
    }
    
    def union(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).union(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def takeOut(): DataTable = {
        val table = DataTable.from(TABLE)
        TABLE.clear()
        table
    }
    
    def takeOut(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            val table = DataTable.from(BUFFER(tableName))
            BUFFER.remove(tableName)
            table
        }
        else {
            DataTable()
        }
    }

    def par: ParArray[DataRow] = {
        TABLE.par
    }

    def firstRow: DataRow = {
        TABLE.firstRow match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def lastRow: DataRow = {
        TABLE.lastRow match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def getRow(i: Int): DataRow = {
        TABLE.getRow(i) match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def getFirstCellStringValue(defaultValue: String = ""): String = {
        TABLE.getFirstCellStringValue(defaultValue)
    }

    def getFirstCellIntValue(defaultValue: Int = 0): Int = {
        TABLE.getFirstCellIntValue(defaultValue)
    }

    def getFirstCellLongValue(defaultValue: Long = 0L): Long = {
        TABLE.getFirstCellLongValue(defaultValue)
    }

    def getFirstCellFloatValue(defaultValue: Float = 0F): Float = {
        TABLE.getFirstCellFloatValue(defaultValue)
    }

    def getFirstCellDoubleValue(defaultValue: Double = 0D): Double = {
        TABLE.getFirstCellDoubleValue(defaultValue)
    }

    def getFirstCellBooleanValue(defaultValue: Boolean = false): Boolean = {
        TABLE.getFirstCellBooleanValue(defaultValue)
    }

    def firstCellStringValue: String = TABLE.getFirstCellStringValue()
    def firstCellIntValue: Int = TABLE.getFirstCellIntValue()
    def firstCellLongValue: Long = TABLE.getFirstCellLongValue()
    def firstCellFloatValue: Float = TABLE.getFirstCellFloatValue()
    def firstCellDoubleValue: Double = TABLE.getFirstCellDoubleValue()
    def firstCellBooleanValue: Boolean = TABLE.getFirstCellBooleanValue()
    
    def discard(tableName: String): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER.remove(tableName)
        }
        this
    }
    
    def nonEmpty: Boolean = {
        TABLE.nonEmpty
    }
    
    def isEmpty: Boolean = {
        TABLE.isEmpty
    }

    def show(limit: Int = 20): DataHub = {
        TABLE.show(limit)

        TO_BE_CLEAR = true

        this
    }
    
    // ---------- buffer action ----------
    
    def label(alias: (String, String)*): DataHub = {
        TABLE.label(alias: _*)
        this
    }
    
    def pass(querySentence: String, default:(String, Any)*): DataHub = {
        if (DEBUG) println(querySentence)
        if (TABLE.isEmpty) {
            if (default.nonEmpty) {
                TABLE.addRow(DataRow(default: _*))
            }
            else {
                throw new Exception("No data to pass. Please ensure data exists or provide default value")
            }
        }
        TABLE.cut(CURRENT.tableSelect(querySentence, TABLE))
        
        this
    }
    
    def foreach(callback: DataRow => Unit): DataHub = {
        TABLE.foreach(callback)
        this
    }
    
    def map(callback: DataRow => DataRow) : DataHub = {
        TABLE.cut(TABLE.map(callback))
        this
    }
    
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataHub = {
        TABLE.cut(TABLE.table(fields: _*)(callback))
        this
    }
    
    def flat(callback: DataTable => DataRow): DataHub = {
        val row = callback(TABLE)
        TABLE.clear()
        TABLE.addRow(row)
        this
    }
    
    def filter(callback: DataRow => Boolean): DataHub = {
        TABLE.cut(TABLE.filter(callback))
        this
    }
    
    def collect(filter: DataRow => Boolean)(map: DataRow => DataRow): DataHub = {
        TABLE.cut(TABLE.collect(filter)(map))
        this
    }
    
    def distinct(fieldNames: String*): DataHub = {
        TABLE.cut(TABLE.distinct(fieldNames: _*))
        this
    }
    
    def count(groupBy: String*): DataHub = {
        TABLE.cut(TABLE.count(groupBy: _*))
        this
    }
    
    def sum(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.sum(fieldName, groupBy: _*))
        this
    }
    
    def avg(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.avg(fieldName, groupBy: _*))
        this
    }
    
    def min(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.min(fieldName, groupBy: _*))
        this
    }
    
    def max(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.max(fieldName, groupBy: _*))
        this
    }
    
    def take(amount: Int): DataHub = {
        TABLE.cut(TABLE.take(amount))
        this
    }

    def takeSample(amount: Int): DataHub = {
        TABLE.cut(TABLE.takeSample(amount))
        this
    }
    
    def insertRow(fields: (String, Any)*): DataHub = {

        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        TABLE.insertRow(fields: _*)
        this
    }
    
    def insertRowIfEmpty(fields: (String, Any)*): DataHub = {

        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        if (TABLE.isEmpty) {
            TABLE.insertRow(fields: _*)
        }
        this
    }
    
    def clear(): DataHub = {
        TABLE.clear()
        this
    }
    
    //def updateRow
    
    // ---------- Json & Api ---------
    
    def openJson(): DataHub = {
        this
    }
    
    def openJson(jsonText: String): DataHub = {
        JSON = Json.fromText(jsonText)
        this
    }
    
    def openJsonApi(url: String): DataHub = {
        JSON = Json.fromURL(url)
        this
    }
    
    def openJsonApi(url: String, post: String): DataHub = {
        JSON = Json.fromURL(url, post)
        this
    }
    
    def find(jsonPath: String): DataHub = {
        TABLE.copy(JSON.findDataTable(jsonPath))
        this
    }
    
    // ---------- dataSource ----------
  
    def executeDataTable(SQL: String, values: Any*): DataTable = CURRENT.executeDataTable(SQL, values: _*)
    def executeDataRow(SQL: String, values: Any*): DataRow = CURRENT.executeDataRow(SQL, values: _*)
    def executeSingleValue(SQL: String, values: Any*): Option[String] = CURRENT.executeSingleValue(SQL, values: _*)
    def executeExists(SQL: String, values: Any*): Boolean = CURRENT.executeExists(SQL, values: _*)
    def executeNonQuery(SQL: String, values: Any*): Int = CURRENT.executeNonQuery(SQL, values: _*)
    
    // ---------- Json Basic ----------
    
    def findDataTable(jsonPath: String): DataTable = JSON.findDataTable(jsonPath)
    def findDataRow(jsonPath: String): DataRow = JSON.findDataRow(jsonPath)
    def findList(jsonPath: String): List[Any] = JSON.findList(jsonPath)
    def findValue(jsonPath: String): Any = JSON.findValue(jsonPath)
    
    
    // ---------- other ----------
    
    def runShell(commandText: String): DataHub = {
        Shell.run(commandText)
        this
    }
    
    def close(): Unit = {

        SOURCES.values.foreach(_.close())
        SOURCES.clear()
        BUFFER.clear()
        TABLE.clear()
        pageSQLs.clear()

        FilePath.delete(holder)
    }
}