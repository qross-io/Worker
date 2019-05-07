package io.qross.psql

class SQLParserException(val s: String) extends RuntimeException(s) {

}

class SQLExecuteException(val s: String) extends RuntimeException(s) {

}