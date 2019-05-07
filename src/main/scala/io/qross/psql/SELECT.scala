package io.qross.psql

import scala.util.{Failure, Success, Try}

class SELECT(statement: Statement, val sentence: String, val name: String, val resultType: String, val retry: String) {

    var retryLimit = 0

    if (retry.nonEmpty) {
        retryLimit = Try(retry
                .toUpperCase()
                .replace("ALWAYS", "0").toInt) match {
            case Success(value) => value
            case Failure(_) => throw new SQLParserException("Wrong TRY times expression: " + retry + ", only supports integer or keyword ALWAYS"); 0
        }
    }
}
