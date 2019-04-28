package io.qross.core

import io.qross.jdbc.DataType.DataType
import io.qross.jdbc.{DataSource, DataType}
import io.qross.ext.Output._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.util.Random
import scala.util.control.Breaks._

object DataTable {
    
    def from(dataTable: DataTable): DataTable = {
        DataTable().copy(dataTable)
    }
    
    def ofSchema(dataTable: DataTable): DataTable = {
        val table = DataTable()
        table.fields ++= dataTable.fields
        table
    }
    
    def withFields(fields: (String, DataType)*): DataTable = {
        val table = DataTable()
        fields.foreach(field => {
            table.addField(field._1, field._2)
        })
        table
    }
}

case class DataTable(private val items: DataRow*) {
    
    private val rows = new mutable.ArrayBuffer[DataRow]()
    private val fields = new mutable.LinkedHashMap[String, DataType]()
    private val labels = new mutable.LinkedHashMap[String, String]()
    
    //initial rows
    for (row <- items) {
        addRow(row)
    }

    def addField(fieldName: String, dataType: DataType): Unit = {
        // . is illegal char in SQLite
        val name = if (fieldName.contains(".")) fieldName.substring(fieldName.lastIndexOf(".") + 1) else fieldName
        fields += name -> dataType
        labels += name -> name
    }

    def addFieldWithLabel(fieldName: String, labelName: String, dataType: DataType): Unit = {
        val name = if (fieldName.contains(".")) fieldName.substring(fieldName.lastIndexOf(".") + 1) else fieldName
        fields += name -> dataType
        labels += name -> labelName
    }
    
    def label(alias: (String, String)*): DataTable = {
        for ((fieldName, otherName) <- alias) {
            labels += fieldName -> otherName
        }
        this
    }
    
    def contains(field: String): Boolean = fields.contains(field)
    
    def addRow(row: DataRow): Unit = {
        for (field <- row.getFields) {
            if (!contains(field)) {
                row.getDataType(field) match {
                    case Some(dataType) => addField(field, dataType)
                    case None =>
                }
            }
        }
        rows += row
    }

    def par: ParArray[DataRow] = {
        rows.par
    }
    
    def foreach(callback: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            callback(row)
        })
        this
    }

    //遍历并返回新的DataTable
    def iterate(callback: DataRow => Unit): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            callback(row)
            table.addRow(row)
        })

        table
    }
    
    def collect(filter: DataRow => Boolean) (map: DataRow => DataRow): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                table.addRow(map(row))
            }
        })

        table
    }
    
    def map(callback: DataRow => DataRow): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            table.addRow(callback(row))
        })

        table
    }
    
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataTable = {
        val table = DataTable.withFields(fields: _*)
        rows.foreach(row => {
            table.merge(callback(row))
        })

        table
    }
    
    def filter(callback: DataRow => Boolean): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            if (callback(row)) {
                table.addRow(row)
            }
        })

        table
    }
    
    def distinct(fieldNames: String*): DataTable = {
        val table = DataTable()
        val set = new mutable.HashSet[DataRow]()
        if (fieldNames.isEmpty) {
            rows.foreach(row => set += row)
        }
        else {
            rows.foreach(row => {
                val newRow = DataRow()
                fieldNames.foreach(fieldName => newRow.set(fieldName, row.get(fieldName).get))
                set += newRow
            })
        }
        set.foreach(row => table.addRow(row))

        table
    }
    
    def count(): Int = rows.size
    
    def count(groupBy: String*): DataTable = {
        val table = DataTable()
        if (groupBy.isEmpty) {
            table.addRow(DataRow("_count" -> count))
        }
        else {
            val map = new mutable.HashMap[DataRow, Int]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + 1)
                }
                else {
                    map.put(newRow, 1)
                }
            })
            for ((row, c) <- map) {
                row.set("_count", c)
                table.addRow(row)
            }
            map.clear()
        }
        table
    }
    
    def sum(fieldName: String, groupBy: String*): DataTable = {
        val table = DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(DataRow("_sum" -> s))
        }
        else {
            val map = new mutable.HashMap[DataRow, Double]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, row.getDoubleOption(fieldName).getOrElse(0D))
                }
            })
            for ((row, s) <- map) {
                row.set("_sum", s)
                table.addRow(row)
            }
            map.clear()
        }
        
        table
    }
    
    //avg
    def avg(fieldName: String, groupBy: String*): DataTable = {
    
        case class AVG(private val v: Double = 0D) {
            
            var count = 0D
            var sum = 0D
            if (v > 0) plus(v)
            
            def plus(v: Double): Unit = {
                count += 1
                sum += v
            }
            
            def get(): Double = {
                if (count == 0) {
                    0D
                }
                else {
                    sum / count
                }
            }
        }
        
        val table = DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(DataRow("_avg" -> s / count))
        }
        else {
            val map = new mutable.HashMap[DataRow, AVG]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).plus(row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, AVG(row.getDoubleOption(fieldName).getOrElse(0D)))
                }
            })
            for ((row, v) <- map) {
                row.set("_avg", v.get())
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //max
    def max(fieldName: String, groupBy: String*): DataTable = {
    
        case class MAX(number: Option[Double] = None) {
            
            var max: Option[Double] = None
            if (number.nonEmpty) compare(number)
            
            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        max = max match {
                            case Some(a) => Some(v max a)
                            case None => Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = max
        }
        
        val table = DataTable()
        if (groupBy.isEmpty) {
            val m = MAX()
            rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(DataRow("_max" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MAX]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MAX(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_max", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //min
    def min(fieldName: String, groupBy: String*): DataTable = {
        
        case class MIN(number: Option[Double] = None) {
        
            var min: Option[Double] = None
            if (number.nonEmpty) compare(number)
        
            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        min = min match {
                            case Some(a) => Some(v min a)
                            case None => Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = min
        }
    
        val table = DataTable()
        if (groupBy.isEmpty) {
            val m = MIN()
            rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(DataRow("_min" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MIN]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MIN(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_min", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //take
    def take(amount: Int): DataTable = {
        val table = DataTable()
        for (i <- 0 until amount) {
            table.addRow(rows(i))
        }
        
        table
    }

    def takeSample(amount: Int): DataTable = {
        val table = DataTable()
        Random.shuffle(rows)
            .take(amount)
            .foreach(row => {
                table.addRow(row)
            })

        table
    }

    def insertRow(fields: (String, Any)*): DataTable = {
        addRow(DataRow(fields: _*))
        this
    }
    
    def updateWhile(filter: DataRow => Boolean)(setValue: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            if (filter(row)) {
                setValue(row)
            }
        })
        this
    }
    
    def upsertRow(filter: DataRow => Boolean)(setValue: DataRow => Unit)(fields: (String, Any)*): DataTable = {
        var exists = false
        breakable {
            for(row <- rows) {
                if (filter(row)) {
                    setValue(row)
                    exists = true
                    break
                }
            }
        }
        if (!exists) {
            insertRow(fields: _*)
        }
        
        this
    }
    
    def deleteWhile(filter: DataRow => Boolean): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            if (!filter(row)) {
                table.addRow(row)
            }
        })
        clear()
        table
    }
    
    def select(filter: DataRow => Boolean)(fieldNames: String*): DataTable = {
        val table = DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                val newRow = DataRow()
                fieldNames.foreach(fieldName => {
                    newRow.set(fieldName, row.get(fieldName).orNull)
                })
                table.addRow(newRow)
            }
        })
        table
    }

    def select(filter: DataRow => Boolean): ArrayBuffer[DataRow] = {
        val rows = new ArrayBuffer[DataRow]()
        this.rows.foreach(row => {
            if (filter(row)) {
                rows += row
            }
        })
        rows
    }
    
    def updateSource(SQL: String): DataTable = {
        updateSource(DataSource.DEFAULT, SQL)
        this
    }
    
    def updateSource(dataSource: String, SQL: String): DataTable = {
        val ds = new DataSource(dataSource)
        ds.tableUpdate(SQL, this)
        ds.close()
        
        this
    }

    def nonEmpty: Boolean = {
        rows.nonEmpty
    }
    
    def isEmpty: Boolean = {
        rows.isEmpty
    }

    def isEmptySchema: Boolean = {
        fields.isEmpty
    }

    def nonEmptySchema: Boolean = {
        fields.nonEmpty
    }
    
    def copy(otherTable: DataTable): DataTable = {
        clear()
        union(otherTable)
        this
    }
    
    def cut(otherTable: DataTable): DataTable = {
        clear()
        merge(otherTable)
        this
    }
    
    def merge(otherTable: DataTable): DataTable = {
        union(otherTable)
        otherTable.clear()
        this
    }
    
    def union(otherTable: DataTable): DataTable = {
        fields ++= otherTable.fields
        labels ++= otherTable.labels
        rows ++= otherTable.rows
        this
    }

    def join(otherTable: DataTable, on: (String, String)*): DataTable = {
        fields ++= otherTable.fields
        labels ++= otherTable.labels
        for (row <- this.rows) {
            for (line <- otherTable.rows) {
                var matched = true
                breakable {
                    for (pair <- on) {
                        if (row.getString(pair._1) != line.getString(pair._2)) {
                            matched = false
                            break
                        }
                    }
                }
                if (matched) {
                    row.combine(line)
                }
            }
        }
        otherTable.clear()
        this
    }

    def batchUpdate(dataSource: DataSource, nonQuerySQL: String): Boolean = {
        if (nonEmpty) {
            dataSource.tableUpdate(nonQuerySQL, this)
            true
        }
        else {
            false
        }
    }
    
    def getFieldNames: List[String] = fields.keySet.toList
    def getLabelNames: List[String] = labels.values.toList
    def getLabels: mutable.LinkedHashMap[String, String] = labels
    def getFields: mutable.LinkedHashMap[String, DataType] = fields
    def getRows: mutable.ArrayBuffer[DataRow] = rows
    def getRow(i: Int): Option[DataRow]= if (i < rows.size) Some(rows(i)) else None

    def firstRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(0)) else None
    def lastRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(rows.size - 1)) else None

    def getFirstCellStringValue(defaultValue: String = ""): String = {
        firstRow match {
            case Some(row) => row.getFirstString(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellIntValue(defaultValue: Int = 0): Int = {
        firstRow match {
            case Some(row) => row.getFirstInt(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellLongValue(defaultValue: Long = 0L): Long = {
        firstRow match {
            case Some(row) => row.getFirstLong(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellFloatValue(defaultValue: Float = 0F): Float = {
        firstRow match {
            case Some(row) => row.getFirstFloat(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellDoubleValue(defaultValue: Double = 0D): Double = {
        firstRow match {
            case Some(row) => row.getFirstDouble(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellBooleanValue(defaultValue: Boolean = false): Boolean = {
        firstRow match {
            case Some(row) => row.getFirstBoolean(defaultValue)
            case None => defaultValue
        }
    }

    def firstCellStringValue: String = getFirstCellStringValue()
    def firstCellIntValue: Int = getFirstCellIntValue()
    def firstCellLongValue: Long = getFirstCellLongValue()
    def firstCellFloatValue: Float = getFirstCellFloatValue()
    def firstCellDoubleValue: Double = getFirstCellDoubleValue()
    def firstCellBooleanValue: Boolean = getFirstCellBooleanValue()
    
    def mkString(delimiter: String, fieldName: String): String = {
        val value = new StringBuilder()
        for (row <- rows) {
            if (value.nonEmpty) {
                value.append(delimiter)
            }
            value.append(row.getString(fieldName, "null"))
        }
        value.toString()
    }
    
    def mkString(prefix: String, delimiter: String, fieldName: String, suffix: String): String = {
        prefix + mkString(delimiter, fieldName) + suffix
    }
    
    def show(limit: Int = 20): Unit = {
        writeLine("------------------------------------------------------------------------")
        writeLine(rows.size, " ROWS")
        writeLine("------------------------------------------------------------------------")
        writeLine(getLabelNames.mkString(", "))
        breakable {
            var i = 0
            for (row <- rows) {
                writeLine(row.join(", "))
                i += 1
                if (i >= limit) {
                    break
                }
            }
        }
        writeLine("------------------------------------------------------------------------")
    }
    
    override def toString: String = {
        val sb = new StringBuilder()
        for ((fieldName, dataType) <- fields) {
            if (sb.nonEmpty) {
                sb.append(",")
            }
            sb.append("\"" + fieldName + "\":\"" + dataType + "\"")
        }
        "{\"fields\":{" + sb.toString +"}, \"rows\":" + rows.asJava.toString + "}"
    }
    
    def toJsonString: String = {
        toString
    }
    
    def toHtmlString: String = {
        val sb = new StringBuilder()
        sb.append("""<table cellpadding="5" cellspacing="1" border="0" style="background-color:#909090">""")
        sb.append("<tr>")
        fields.keySet.foreach(field => {
            sb.append("""<th style="text-align: left; background-color:#D0D0D0">""")
            sb.append(labels(field))
            sb.append("</th>")
        })
        sb.append("</tr>")
        rows.foreach(row => {
            sb.append("<tr>")
            row.getFields.foreach(field => {
                sb.append("""<td style="background-color: #FFFFFF;""")
                row.getDataType(field) match {
                    case Some(dt) => if (dt == DataType.DECIMAL || dt == DataType.INTEGER) sb.append(" text-align: right;")
                    case _ =>
                }
                sb.append("""">""")
                sb.append(row.getString(field))
                sb.append("</td>")
            })
            sb.append("</tr>")
        })
        sb.append("</table>")
        
        sb.toString()
    }
    
    def clear(): Unit = {
        rows.clear()
        fields.clear()
        labels.clear()
    }
}
