package io.qross.ext

import java.io.File
import java.util.regex.{Matcher, Pattern}

import io.qross.core.DataCell
import io.qross.setting.Global
import io.qross.time.Timer
import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}

import scala.util.{Success, Try}
import scala.collection.mutable.HashMap
import scala.sys.process._
import scala.util.matching.Regex

object TypeExt {

    implicit class StringExt(var string: String) {

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

        def toHashMap(delimiter: String = "&", terminator: String = "="): Map[String, String] = {
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
                    new DataCell(null)
            }

            //The code below doesn't work.
            //import scala.tools.nsc._
            //val interpreter = new Interpreter(new Settings())
            //interpreter.interpret("import java.text.{SimpleDateFormat => sdf}")
            //interpreter.bind("date", "java.util.Date", new java.util.Date());
            //interpreter.eval[String]("""new sdf("yyyy-MM-dd").format(date)""") get
            //    interpreter.close()
        }

        def call(): DataCell = {
            val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
            try {
                new DataCell(jse.eval(s"""(function(){ $string })()"""))
            }
            catch {
                case e: ScriptException =>
                    e.printStackTrace()
                    new DataCell(null)
            }
        }

        //去掉变量修饰符
        def removeVariableModifier(): String = {
            string.replace("$", "").replace("{", "").replace("}", "").trim
        }

        //为计算值添加双引号，用于PSQL计算过程中
        def useQuotes(): String = {
            "\"" + string.replace("\"", "\\\"") + "\""
        }

        def useQuotesIf(condition: Boolean): String = {
            if (condition) {
                string.useQuotes()
            }
            else {
                string
            }
        }

        //去掉常量中的双引号，用于PSQL计算结果
        def removeQuotes(): String = {
            if ((string.startsWith("\"") && string.endsWith("\"")) || (string.startsWith("'") && string.endsWith("'"))) {
                string.substring(1, string.length - 1).replace("\\\"", "\"")
            }
            else {
                string
            }
        }

        def $trim(prefix: String, suffix: String): String = {
            string = string.trim
            if (string.startsWith(prefix)) {
                string = string.drop(1)
            }
            if (string.endsWith(suffix)) {
                string = string.dropRight(1)
            }
            string
        }

        def $quote(char: String): String = {
            if (!string.startsWith(char)) {
                string = char + string
            }
            else if (!string.endsWith(char)) {
                string += char
            }

            string
        }

        def takeBefore(char: Any): String = {
            char match {
                case char: String => string.take(string.indexOf(char))
                case index: Integer => string.take(index)
                case rex: Regex => string.take(string.indexOf(rex.findFirstIn(string).getOrElse("")))
                case _ => ""
            }
        }

        def takeAfter(char: Any): String = {
            char match {
                case char: String =>
                    if (char == "") {
                        ""
                    }
                    else {
                        string.substring(string.indexOf(char) + char.length)
                    }
                case index: Integer => string.substring(index + 1)
                case rex: Regex =>
                    rex.findFirstIn(string) match {
                        case Some(v) => string.substring(string.indexOf(v) + v.length)
                        case None => ""
                    }
                case _ => ""
            }
        }

        def $replace(regex: String, newChar: String): String = {
            val p = Pattern.compile(regex)
            var m: Matcher = null
            if ({m = p.matcher(newChar); m}.find()) {
                p.matcher(string).replaceAll(newChar)
            }
            else {
                while ({m = p.matcher(string); m}.find()) {
                    string = m.replaceAll(newChar)
                }
                string
            }
        }

        def ifEmpty(defaultValue: String): String = {
            if (string == "") {
                defaultValue
            }
            else {
                string
            }
        }

        def ifNull(defaultValue: String): String = {
            if (string == null) {
                defaultValue
            }
            else {
                string
            }
        }

        def ifNullOrEmpty(defaultValue: String): String = {
            if (string == null || string == "") {
                defaultValue
            }
            else {
                string
            }
        }

        def bash(): Int = {
            val exitValue = string.!(ProcessLogger(out => {
                println(out)
            }, err => {
                System.err.println(err)
                //println(err)
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

            //println("exitValue: " + process.exitValue())
            //process.destroy()

            process.exitValue()
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

    implicit class FloatExt(float: Float) {
        def floor(precision: Int = 0): Double = {
            Math.floor(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }
    }

    implicit class DoubleExt(double: Double) {
        def floor(precision: Int = 0): Double = {
            Math.floor(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }
    }

    implicit  class RegexExt(regex: Regex) {
        def test(str: String): Boolean = {
            regex.findFirstIn(str).nonEmpty
        }
    }

//    implicit class ListExt[A](list: List[A]) {
//        def first: Option[A] = if (list.nonEmpty) Some(list.head) else None
//    }
}
