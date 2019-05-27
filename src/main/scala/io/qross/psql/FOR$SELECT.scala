package io.qross.psql

import io.qross.core.{DataHub, DataRow}
import io.qross.ext.TypeExt._

class FOR$SELECT(var variable: String, selectSQL: String) {

    private val fields = variable.split(",").map(v => {
        if (!v.trim.startsWith("$")) {
            throw new SQLParseException("In FOR SELECT, loop variable must start with '$'.")
            ""
        }
        else {
            v.trim.takeAfter(0).$trim("(", ")")
        }
    })

    def computeMap(dh: DataHub): ForLoopVariables = {
        val loopVars = new ForLoopVariables()

        val table = dh.executeDataTable(selectSQL)
        if (fields.length <= table.columnCount) {
            table.map(row => {
                val newRow = DataRow()
                for (i <- fields.indices) {
                    newRow.set(fields(i).toUpperCase, row.get(i).orNull)
                }
                newRow
            }).foreach(loopVars.addRow)
        }
        else {
            throw new SQLParseException("In FOR SELECT loop, result columns must equal or more than variables amount.")
        }

        loopVars
    }
}