package io.qross.sql

import io.qross.ext.TypeExt._

object OUT {
    val ALL: String = "ALL"
    val LIST: String = "LIST"
    val ARRAY: String = "ARRAY"
    val MAP: String = "MAP"
    val SINGLE: String = "SINGLE"
    val AFFECTED: String = "AFFECTED"
}

class OUT(var outputType: String, val outputName: String, val SQL: String) {
    val caption: String = SQL.takeBefore("""\s""".r).toUpperCase()
    if (outputType == null) {
        outputType = if (caption == "SELECT") {
            OUT.LIST
        }
        else {
            OUT.AFFECTED
        }
    }
}