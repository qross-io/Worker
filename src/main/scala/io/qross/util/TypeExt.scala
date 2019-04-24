package io.qross.util

import io.qross.core.DataCell
import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}

import scala.util.{Success, Try}
import scala.collection.mutable.HashMap
import scala.sys.process._

object TypeExt {

    implicit class StringExt(string: String) {

        def toBooleanOrElse(defaultValue: Boolean = false): Boolean = {
            if (Set("yes", "true", "1", "on", "ok").contains(string)) {
                true
            }
            else if (Set("no", "false", "0", "off", "cancel").contains(string)) {
                false
            }
            else {
                defaultValue
            }
        }

        def toPath: String = {
            string.replace("\\", "/")
        }

        def toDir: String = {
            var dir = string.replace("\\", "/")
            if (!string.endsWith("/")) {
                dir += "/"
            }
            dir
        }

        def toQrossPath: String = {
            string.replace("\\", "/")
                    .replace("%USER_HOME", Global.USER_HOME)
                    .replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
                    .replace("%QROSS_HOME", Global.QROSS_HOME)
                    .replace("%QROSS_KEEPER_HOME", Global.QROSS_KEEPER_HOME)
                    .replace("%QROSS_WORKER_HOME", Global.QROSS_WORKER_HOME)
                    .replace("//", "/")
        }

        def toByteLength: Long = {
            var number = string.dropRight(1)
            var times: Long = string.takeRight(1).toUpperCase() match {
                case "B" => 1
                case "K" => 1024
                case "M" => 1024 * 1024
                case "G" => 1024 * 1024 * 1024
                case "T" => 1024 * 1024 * 1024 * 1024
                case "P" => 1024 * 1024 * 1024 * 1024 * 1024
                case b =>
                    Try(b.toInt) match {
                        case Success(n) => number += n
                        case _ =>
                    }
                    1
            }

            Try(number.toLong) match {
                case Success(n) => n * times
                case _ => 0
            }
        }

        def toMap(delimiter: String = "&", terminator: String = "="): Map[String, String] = {
            val params = string.split(delimiter)
            val queries = new HashMap[String, String]()
            for (param <- params) {
                if (param.contains(terminator)) {
                    queries += param.substring(0, param.indexOf(terminator)) -> param.substring(param.indexOf(terminator) + 1)
                }
                else {
                    queries += param -> ""
                }
            }
            queries.toMap
        }

        def eval(): DataCell = {
            val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
            try {
                new DataCell(jse.eval(string))
            }
            catch {
                case e: ScriptException =>
                    e.printStackTrace()
                    null
            }

            //The code below doesn't work.
            //import scala.tools.nsc._
            //val interpreter = new Interpreter(new Settings())
            //interpreter.interpret("import java.text.{SimpleDateFormat => sdf}")
            //interpreter.bind("date", "java.util.Date", new java.util.Date());
            //interpreter.eval[String]("""new sdf("yyyy-MM-dd").format(date)""") get
            //    interpreter.close()
        }

        def bash(): Int = {
            val exitValue = string.!(ProcessLogger(out => {
                println(out)
            }, err => {
                //System.err.println(err)
                println(err)
            }))

            exitValue
        }

        def go(): Int = {

            val process = string.run(ProcessLogger(out => {
                println(out)
            }, err => {
                System.err.println(err)
            }))

            var i = 0
            while(process.isAlive()) {
                println("s#" + i)
                i += 1
                //            if (i > 10) {
                //                process.destroy()
                //            }
                Timer.sleep(1)
            }

            println("exitValue: " + process.exitValue())
            //process.destroy()

            0
        }
    }

    implicit class LongExt(long: Long) {
        def toHumanized: String = {
            var size: Double = long
            val units = List("B", "K", "M", "G", "T", "P")
            var i = 0
            while (size > 1024 && i < 6) {
                size /= 1024
                i += 1
            }

            f"$size%.2f${units(i)}"
        }
    }
}
