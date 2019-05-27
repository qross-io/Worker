package io.qross.psql

import java.util.regex.Matcher

import io.qross.core.DataRow
import io.qross.ext.PlaceHolder
import io.qross.ext.TypeExt._
import io.qross.ext.PlaceHolder._

import scala.collection.mutable.ArrayBuffer

class FOR$IN(val forItems: String, forCollection: String, val delimiter: String) {

    val fields = new ArrayBuffer[String]()
    val separators = new ArrayBuffer[String]()

    private var declarations = forItems

    USER_VARIABLE.r
            .findAllMatchIn(forItems)
            .map(m => m.group(0))
            .foreach(field => fields += field)

    for (i <- fields.indices) {
        if (i > 0) {
            separators += declarations.takeBefore(fields(i))
        }
        else {
            separators += ""
        }
        declarations = declarations.takeAfter(fields(i))
    }

    if (!declarations.isEmpty) {
        separators += declarations
    }
    else {
        separators += ""
    }

    def computeMap(PSQL: PSQL): ForLoopVariables = {
        val variablesMaps = new ForLoopVariables()
        val collection = this.forCollection.$eval(PSQL).split(delimiter, -1)
        for (l <- collection) {
            var line = l
            val row = new DataRow()
            for (j <- fields.indices) {
                val field = fields(j).toUpperCase
                val prefix = separators(j)
                val suffix = separators(j + 1)

                line = line.substring(prefix.length)
                if (suffix.isEmpty) {
                    row.set(field, line)
                    //line = ""; //set empty
                }
                else {
                    row.set(field, line.substring(0, line.indexOf(suffix)))
                    line = line.substring(line.indexOf(suffix))
                }
            }
            variablesMaps.addRow(row)
        }
        variablesMaps
    }
}
