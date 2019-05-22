package io.qross.psql

import io.qross.core.{DataCell, DataRow}

import scala.collection.mutable.ArrayBuffer

class ForLoopVariables {
    //循环列表项及值
    private val variables = new ArrayBuffer[DataRow]
    private var cursor = -1

    def addRow(row: DataRow): Unit = {
        this.variables += row
    }

    def hasNext: Boolean = {
        this.cursor += 1
        this.cursor < variables.size
    }

    def contains(field: String): Boolean = {
        if (this.variables.nonEmpty) {
            this.variables.head.contains(field)
        }
        else {
            false
        }
    }

    def get(field: String): DataCell = this.variables(this.cursor).getCell(field)

    def set(field: String, value: Any): Unit = {
        this.variables(this.cursor).set(field, value)
    }
}