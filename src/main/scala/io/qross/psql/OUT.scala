package io.qross.psql

import io.qross.ext.TypeExt._

class OUT(var outputType: String, val outputName: String, val SQL: String) {
    val caption: String = SQL.takeBefore("""\s""".r).toUpperCase()
    if (outputType == null) {
        outputType = if (caption == "SELECT") {
            "LIST"
        }
        else {
            "AFFECTED"
        }
    }
}