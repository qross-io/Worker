package io.qross.psql

class FOR$TO(val variable: String, rangeBegin: String, rangeEnd: String) {

    def parseBegin(statement: Statement): Integer = {
        statement.parseStandardSentence(rangeBegin).toInt
    }

    def parseEnd(statement: Statement): Integer = {
        statement.parseStandardSentence(rangeEnd).toInt
    }

    def hasNext(PSQL: PSQL, statement: Statement): Boolean = {
        PSQL.findVariableValue(variable).value.asInstanceOf[String].toInt <= parseEnd(statement)
    }
}
