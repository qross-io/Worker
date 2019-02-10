package io.qross.util

import com.fasterxml.jackson.databind.ObjectMapper
import io.qross.util.DataType.DataType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DataRow {
   
    def from(json: String): DataRow = Json(json).findDataRow("/")
    def from(json: String, path: String): DataRow = Json(json).findDataRow(path)
    def from(row: DataRow, fieldNames: String*): DataRow = {
        val newRow = DataRow()
        if (fieldNames.nonEmpty) {
            fieldNames.foreach(fieldName => newRow.set(fieldName, row.get(fieldName).get))
        }
        else {
            newRow.columns ++= row.columns
            newRow.fields ++= row.fields
        }
        newRow
    }
}

case class DataRow(private val items: (String, Any)*) {
    
    val columns = new mutable.LinkedHashMap[String, Any]()
    val fields = new mutable.LinkedHashMap[String, DataType]()
    
    for ((k, v) <- items) {
        this.set(k, v)
    }
    
    //insert or update
    def set(fieldName: String, value: Any): Unit = {
        this.columns += fieldName -> value
        if (!this.fields.contains(fieldName)) {
            this.fields += fieldName -> DataType.of(value)
        }
    }
    
    def remove(fieldName: String): Unit = {
        this.fields.remove(fieldName)
        this.columns.remove(fieldName)
    }
    
    def updateFieldName(fieldName: String, newFieldName: String): Unit = {
        this.set(newFieldName, this.columns.get(fieldName))
        this.remove(fieldName)
    }
    
    def getDataType(fieldName: String): Option[DataType] = {
        if (this.fields.contains(fieldName)) {
            Some(this.fields(fieldName))
        }
        else {
            None
        }
    }
    
    def foreach(callback: (String, Any) => Unit): Unit = {
        for ((k, v) <- this.columns) {
            callback(k, v)
        }
    }
    
    def get(fieldName: String): Option[Any] = {
        if (this.columns.contains(fieldName)) {
            this.columns.get(fieldName)
        }
        else {
            None
        }
    }

    def combine(otherRow: DataRow): DataRow = {
        for ((field, value) <- otherRow.columns) {
            this.set(field, value)
        }
        this
    }
    
    def getString(fieldName: String, defaultValue: String = "NULL"): String = this.get(fieldName) match {
        case Some(value) => if (value != null) value.toString else defaultValue
        case None => defaultValue
    }
    
    def getInt(fieldName: String, defaultValue: Int = 0): Int = getIntOption(fieldName).getOrElse(defaultValue)
    def getIntOption(fieldName: String): Option[Int] = {
        get(fieldName) match {
            case Some(value) => value match {
                    case v: Int => Some(v)
                    case other => Try(other.toString.toDouble.toInt) match {
                            case Success(v) => Some(v)
                            case Failure(_) => None
                        }
                }
            case None => None
        }
    }
    
    def getLong(fieldName: String, defaultValue: Long = 0L): Long = getLongOption(fieldName).getOrElse(defaultValue)
    def getLongOption(fieldName: String): Option[Long] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Long => Some(v)
                case other => Try(other.toString.toDouble.toLong) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    
    def getFloat(fieldName: String, defaultValue: Float = 0F): Float = getFloatOption(fieldName).getOrElse(defaultValue)
    def getFloatOption(fieldName: String): Option[Float] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Float => Some(v)
                case other => Try(other.toString.toFloat) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    
    def getDouble(fieldName: String, defaultValue: Double = 0D): Double = getDoubleOption(fieldName).getOrElse(defaultValue)
    def getDoubleOption(fieldName: String): Option[Double] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Long => Some(v)
                case v: Float => Some(v)
                case v: Double => Some(v)
                case other => Try(other.toString.toDouble) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    
    def getBoolean(fieldName: String): Boolean = {
        val value = getString(fieldName, "0").toLowerCase
        value == "yes" || value == "true" || value == "1" || value == "ok"
    }

    def getFields: List[String] = this.fields.keySet.toList
    def getDataTypes: List[DataType] = this.fields.values.toList
    def getValues: List[Any] = this.columns.values.toList

    def getFirstString(defaultVlaue: String = ""): String = {
        this.columns.headOption match {
            case Some(field) => field._2.toString
            case None => defaultVlaue
        }
    }

    def getFirstInt(defaultValue: Int = 0): Int = {
        this.fields.headOption match {
            case Some(field) => this.getInt(field._1)
            case None => defaultValue
        }
    }

    def getFirstLong(defaultValue: Long = 0L): Long = {
        this.fields.headOption match {
            case Some(field) => this.getLong(field._1)
            case None => defaultValue
        }
    }

    def getFirstFloat(defaultValue: Float = 0F): Float = {
        this.fields.headOption match {
            case Some(field) => this.getFloat(field._1)
            case None => defaultValue
        }
    }

    def getFirstDouble(defaultValue: Double = 0D): Double = {
        this.fields.headOption match {
            case Some(field) => this.getDouble(field._1)
            case None => defaultValue
        }
    }

    def getFirstBoolean(defaultValue: Boolean = false): Boolean = {
        this.fields.headOption match {
            case Some(field) => this.getBoolean(field._1)
            case None => defaultValue
        }
    }


    
    def contains(fieldName: String): Boolean = this.columns.contains(fieldName)
    def contains(fieldName: String, value: Any): Boolean = this.columns.contains(fieldName) && this.getString(fieldName) == value.toString
    def size: Int = this.columns.size
    
    def join(delimiter: String): String = {
        val values = new mutable.StringBuilder()
        for (field <- getFields) {
            if (values.nonEmpty) {
                values.append(", ")
            }
            values.append(this.getString(field, "null"))
        }
        values.toString()
    }

    override def toString: String = {
        new ObjectMapper().writeValueAsString(this.columns.asJava)
    }
    
    override def equals(obj: scala.Any): Boolean = {
        this.columns == obj.asInstanceOf[DataRow].columns
    }
    
    def clear(): Unit = {
        this.columns.clear()
        this.fields.clear()
    }
}