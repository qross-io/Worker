package io.qross.ext

import scala.collection.mutable

case class PlaceHolder(character: String) {

    private var symbol: String = character
    if (symbol == "#" || symbol == "$") {
        symbol = "\\" + symbol
    }
    private val expression = s"""[^${this.symbol}]${this.symbol}\\{?([a-zA-Z_][a-zA-Z0-9_]+)\\}?""";

    def findIn(SQL: String): Option[String] = {
        expression.r.findFirstMatchIn(SQL) match {
            case Some(m) => Some(m.group(1))
            case None => None
        }
    }

    def findAllIn(SQL: String): Set[String] = {
        val fields = new mutable.HashSet[String]()
        expression.r.findAllMatchIn(SQL).foreach(m => {
            fields += m.group(1)
        })
        fields.toSet
    }
}
