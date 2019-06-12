package io.qross.sql

import java.util.regex.{Matcher, Pattern}
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._

import scala.collection.mutable.ArrayBuffer

object Function {
    val NAMES = Set(
        "CONCAT", "CHARINDEX", "INSTR", "POSITION", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "LOWER", "UPPER", "TRIM", "SPLIT", "LEN",
        "IFNULL", "NVL",
        "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE")

    def CONCAT(args: String*): String = {
        args.mkString("").useQuotes()
    }

    def POSITION(args: String*): String = {
        val $in = """\sIN\s""".r
        if ($in.test(args(0))) {
            (args(0).takeBefore($in).trim().indexOf(args(0).takeAfter($in)) + 1).toString
        }
        else {
            throw new SQLParseException("Wrong arguments, correct format is POSITION(strA IN strB) , actual " + args(0))
        }
    }

    def INSTR(args: String*): String = {
        String.valueOf(args(0).indexOf(args(1)) + 1)
    }

    def CHARINDEX(args: String*): String = {
        ""
    }

    def REPLACE(args: String*): String = {
        args(0).replace(args(1), args(2)).useQuotes()
    }
}

case class Function(function: String) {

    def call(): String = {
        var m: Matcher = null
        var $func = function
        //子函数
        while ({m = $FUNCTION.matcher($func); m}.find) {
            $func = $func.replace(m.group, this.execute(m.group(1), m.group(2)))
        }
        $func
    }

    private def execute(functionName: String, arguments: String): String = {

        val strings = new ArrayBuffer[String]()

        val $func = functionName.trim.toUpperCase
        var $args = arguments
        var m: Matcher = null
        //替换掉所有的字符串，替换为 #{sx}
        if ($args.contains("\"")) {
            $args = $args.replace("\\\"", "\\&QUOT;")
            val p = Pattern.compile("\"[^\"]+\"")
            m = p.matcher($args)
            while (m.find) {
                $args = $args.replace(m.group(0), "#{s" + strings.size + "}")
                strings += m.group(0).replace("\\&QUOT;", "\\\"")
            }
            $args = $args.replace("\\&QUOT;", "\\\"")
        }

        //按逗号得到参数列表
        val args = $args.split(",")
        for (i <- args.indices) {
            m = $STRING.matcher(args(i))
            while (m.find) {
                args(i) = args(i).replace(m.group(0), strings(m.group(1).toInt))
                args(i).eval()
                    .ifNotNull(cell => {
                        args(i) = cell.value.toString //这里不带括号
                    })
                    .ifNull(() => throw new SQLParseException("Wrong function argument or expression: " + args(i)))
            }
        }

        //按参数类型确定是否带括号
        Function.getClass.getDeclaredMethod($func).invoke(null, args: _*).asInstanceOf[String]
    }
}