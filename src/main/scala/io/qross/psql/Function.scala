package io.qross.psql

import java.util.regex.{Matcher, Pattern}
import io.qross.ext.TypeExt._
import io.qross.psql.Patterns._

import scala.collection.mutable.ArrayBuffer

object Function {
    val NAMES = Set(
        "CONCAT", "CHARINDEX", "INSTR", "POSITION", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "LOWER", "UPPER", "TRIM", "SPLIT", "LEN",
        "IFNULL", "NVL",
        "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE",
        "PERMISSION", "TEST")
}

class Function(statement: Statement) {

    private val strings = new ArrayBuffer[String]

    //$FUNC("c,a,b", , );
    def execute(function: String): String = {
        var m: Matcher = null
        var $func = function
        //子函数
        while ({m = $_FUNCTION.matcher($func); m}.find) {
            $func = $func.replace(m.group, this.execute(m.group(1), m.group(2)))
        }
        $func
    }

    def execute(functionName: String, arguments: String): String = {
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
        if ($func == "POSITION") {
            m = Pattern.compile("[\\s\"](IN)[\\s\"]", Pattern.CASE_INSENSITIVE).matcher($args)
            if (m.find) {
                $args = $args.substring(0, $args.indexOf(m.group)) + m.group.replace(m.group(1), ",") + $args.substring($args.indexOf(m.group) + m.group.length)
            }
        }
        //按逗号得到参数列表
        val args = $args.split(",")
        for (i <- args.indices) {
            m = $STRING.matcher(args(i))
            while (m.find) {
                args(i) = args(i).replace(m.group(0), strings(m.group(1).toInt))
                args(i).eval() match {
                    case Some(data) => args(i) = data.value.toString //这里不带括号
                    case None => throw new SQLParserException("Wrong function argument or expression: " + args(i))
                }
            }
        }
        //按参数类型确定是否带括号
        $func match {
            case "CONCAT" =>
                args.mkString("").useQuotes()
            case "CHARINDEX" => ""
            case "POSITION" =>
                if (args.length == 2) {
                    String.valueOf(args(1).indexOf(args(0)) + 1)
                }
                else {
                    throw new SQLParserException("Need 2 arguments, actual " + args.length)
                }
            case "INSTR" =>
                String.valueOf(args(0).indexOf(args(1)) + 1)
            case "REPLACE" =>
                args(0).replace(args(1), args(2)).useQuotes()
            case "PERMISSION" =>
                //val userName = this.statement.PSQL.findVariableValue("USERNAME")
                //val apiName = this.statement.PSQL.findVariableValue("API_NAME")
                //if (userName != null && apiName != null) return "\"" + Statement.useQuotes(userName.value.toString + "-" + apiName.value.toString) + "\""
                //else return "\"NO_USER\""
                ""
            case "TEST" =>
                "\"TEST\""
        }
    }
}