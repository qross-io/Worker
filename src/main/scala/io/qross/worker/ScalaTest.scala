package io.qross.worker

import io.qross.setting.Properties
import io.qross.pql.PQL
import io.qross.ext.TypeExt._

object ScalaTest {
    def main(args: Array[String]): Unit = {
        PQL.runFile("""/sql/test.sql""")
    }
}
