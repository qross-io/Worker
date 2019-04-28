package io.qross.psql

import java.util.regex.Matcher

import io.qross.core.DataRow
import io.qross.ext.TypeExt._

import scala.collection.mutable.ArrayBuffer

class ForInLoop(statement: Statement, var forItems: String, forCollection: String, var delimiter: String) {

    delimiter = delimiter.removeQuotes()

    val fields = new ArrayBuffer[String]()
    val separators = new ArrayBuffer[String]()

    //both supports ${var} and $var
    val m: Matcher = Patterns.$VARIABLE.matcher(forItems)
    while (m.find) {
        val name = m.group
        val index = forItems.indexOf(name)
        if (index > 0) {
            separators += forItems.substring(0, index)
        }
        else {
            separators += ""
        }
        fields += name.removeVariableModifier()
        forItems = forItems.substring(index + name.length)
    }

    if (!forItems.isEmpty) {
        separators += forItems
    }
    else {
        separators += ""
    }

    def computeMap(): ForLoopVariables = {
        val variablesMaps = new ForLoopVariables
        val collection = statement.parseStandardSentence(this.forCollection).split(this.delimiter, -1)
        for (l <- collection) {
            var line = l
            val row = new DataRow()
            for (j <- fields.indices) {
                val field = fields(j)
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
