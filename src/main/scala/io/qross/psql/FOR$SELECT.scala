package io.qross.psql

import io.qross.core.{DataHub, DataRow}
import io.qross.ext.TypeExt._

class FOR$SELECT(var variable: String, selectSQL: String) {

    private val variables = variable.split(",").map(v => v.removeVariableModifier().trim)

    def computeMap(dh: DataHub): ForLoopVariables = {
        val loopVars = new ForLoopVariables()

        val table = dh.executeDataTable(selectSQL)
        if (variables.length <= table.columnCount) {
            table.map(row => {
                val newRow = DataRow()
                for (i <- variables.indices) {
                    newRow.set(variables(i), row.get(i).orNull)
                }
                newRow
            }).foreach(loopVars.addRow)
        }
        else {
            throw new SQLParserException("In FOR SELECT loop, result columns must equal or more than amount of variables.")
        }

        loopVars
    }
}
