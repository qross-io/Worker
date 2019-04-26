package io.qross.psql

import io.qross.util.TypeExt._

class ForToLoop(statement: Statement, var variable: String, rangeBegin: String, rangeEnd: String) {
    variable = variable.removeVariableModifier()

    def parseBegin: Integer = {
        statement.parseStandardSentence(rangeBegin).toInt
    }

    def parseEnd: Integer = {
        statement.parseStandardSentence(rangeEnd).toInt
    }

    def hasNext: Boolean = {
        statement.PSQL.findVariableValue(variable).value.asInstanceOf[String].toInt <= parseEnd
    }
}
