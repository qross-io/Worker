package io.qross.psql

class SQLParseException(val s: String) extends RuntimeException(s) {

}

class SQLExecuteException(val s: String) extends RuntimeException(s) {

}