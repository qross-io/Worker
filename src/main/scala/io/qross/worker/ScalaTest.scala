package io.qross.worker

import io.qross.setting.Properties
import io.qross.sql.PSQL
import io.qross.ext.TypeExt._

object ScalaTest {
    def main(args: Array[String]): Unit = {
        PSQL.runFile("""/sql/test.sql""")
    }
}
