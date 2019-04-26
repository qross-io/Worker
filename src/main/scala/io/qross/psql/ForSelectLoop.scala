package io.qross.psql

import io.qross.jdbc.DataSource
import io.qross.util.TypeExt._

class ForSelectLoop(statement: Statement, var variable: String, selectSQL: String) {
    private val variables = variable.removeVariableModifier().split(",")

    def computeMap(ds: DataSource): ForLoopVariables = {
        val variables = new ForLoopVariables()

        ds.executeDataTable(selectSQL).foreach(variables.addRow)

        variables
    }
}
