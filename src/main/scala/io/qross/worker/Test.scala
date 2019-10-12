package io.qross.worker

import io.qross.jdbc.DataSource
import io.qross.ext.TypeExt._
import io.qross.fs.FileReader

import scala.io.Source

object Test {
    def main(args: Array[String]): Unit = {
        println("-----------------------------------------------------------------")
        println("-- UTF-8 ---------------------------------------------------------")
        println("-----------------------------------------------------------------")
        Source.fromFile("f:/case_trigger_replace_new.sql", "UTF-8").mkString.print
        println("-----------------------------------------------------------------")
        println("----- utf-8 ---------------------------------------------------------")
        println("-----------------------------------------------------------------")
        Source.fromFile("f:/case_trigger_replace_new.sql", "utf-8").mkString.print
        println("-----------------------------------------------------------------")
        println("----FileReader----------------------------------------------------")
        println("-----------------------------------------------------------------")
        new FileReader("f:/case_trigger_replace_new.sql").readToEnd(!_.startsWith("--")).print
        println("-----------------------------------------------------------------")
        println("----no charset-------------------------------------------------------")
        println("-----------------------------------------------------------------")
        Source.fromFile("f:/case_trigger_replace_new.sql").mkString.print
    }
}
