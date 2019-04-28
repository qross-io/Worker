package io.qross.ext

import io.qross.core.DataRow
import io.qross.jdbc.DataType

import scala.collection.mutable

case class Parameter(private val SQL: String) {
    
    private val fields = new mutable.TreeMap[String, String]()
    //private var symbol = "\\#"
    private val symbols = List("\\#", "\\$") //, "@")

    def matched: Boolean = fields.nonEmpty

    /*
    def find(symbol: String): Parameter = {
        this.symbol = symbol
        if (this.symbol == "#" || this.symbol == "$") {
            this.symbol = "\\" + this.symbol
        }
        fields ++= s"""[^${this.symbol}]${this.symbol}\\{?([a-zA-Z_][a-zA-Z0-9_]+)\\}?""".r.findAllMatchIn(this.SQL).map(m => m.group(1))
        this
    }

    def replaceWith(row: DataRow): String = {
        var replacement = this.SQL
        fields.foreach(field => {
            if (row.contains(field)) {
                replacement = replacement.replace(this.symbol + field, row.getString(field).replace("'", "''"))
            }
        })
        replacement = replacement.replace(this.symbol + this.symbol, this.symbol)

        replacement
    }

    */

    symbols.foreach(symbol => {
        fields ++= s"""[^$symbol]($symbol\\{?([a-zA-Z_][a-zA-Z0-9_]+)\\}?)""".r.findAllMatchIn(this.SQL).map(m => {
            (m.group(1), m.group(2))
        })
    })

    def replaceWith(row: DataRow): String = {

        var replacement = this.SQL

        fields.keys.toList.reverse.foreach(placeHolder => {

            val fieldName = fields(placeHolder)

            if (placeHolder.startsWith("#")) {
                //#
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(placeHolder, row.getString(fieldName))
                }
            }
            else if (placeHolder.startsWith("$")) {
                // $
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(placeHolder, (row.getDataType(fieldName), row.get(fieldName)) match {
                        case (Some(dataType), Some(value)) =>
                            if (value == null) {
                                "NULL"
                            }
                            else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                value.toString
                            }
                            else {
                                "'" + value.toString.replace("'", "''") + "'"
                            }
                        case _ => ""
                    })
                }
            }
            else if (placeHolder.startsWith("@")) {
                //do nothing
            }
        })

        replacement = replacement.replace("##", "#")
        replacement = replacement.replace("$$", "$")
        //replacement = replacement.replace("@@", "@")

        replacement
    }
}