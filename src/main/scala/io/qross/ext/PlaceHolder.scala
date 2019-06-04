package io.qross.ext

import java.util.regex.{Matcher, Pattern}

import io.qross.core.{DataCell, DataRow}
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataType
import io.qross.psql.{Function, GlobalVariable, PSQL, SQLExecuteException}

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.matching.Regex.Match

/*

#{name} 或 &{name}

#name 或 #(name)
&name 或 &(name)

$name 或 $(name)

$func()

${express}

@name 或 @(name)

@func()

~{ js sentence } 或 ~{{ js statement }}

*/

object PlaceHolder {
    //0 whole match 1 whole place holder 2 prefix 3 variable name
    val ARGUMENT: String = """(^|[^#&])(([#&])\{([a-zA-Z0-9_]+)\})"""  //入参 #{name} 或 &{name}
    val PARAMETER: String = """(^|[^#&])(([#&])\(?([a-zA-Z0-9_]+)\)?)""" //DataHub传递参数, #name 或 #{name} 或 &name 或 &(name)
    val USER_VARIABLE: String = """(^|[^\$])(\$\(?([a-zA-Z0-9_]+)\)?)""" //#用户变量
    val GLOBAL_VARIABLE: String = """(^|[^@])(@\(?([a-zA-Z0-9_]+)\)?)""" //#全局变量
    val USER_DEFINED_FUNCTION: String = """$[a-zA-Z_]+\(\)""" //未完成, 用户函数
    val SHARP_EXPRESSION: String = s"""${}""" //未完成, Sharp表达式
    val SYSTEM_FUNCTION: String = raw"""@(${Function.NAMES.mkString("|")})\s*\(""" //系统函数
    val JS_EXPRESSION: String = """\~\{(.+?)}""" //js表达式
    val JS_STATEMENT: String = """\~\{\{(.+?)}}"""// js语句块

    implicit class Sentence(var sentence: String) {

        def hasParameters: Boolean = {
            PARAMETER.r.test(sentence)
        }

        def hasJsExpressions: Boolean = {
            JS_EXPRESSION.r.test(sentence)
        }

        def hasJsStatements: Boolean = {
            JS_STATEMENT.r.test(sentence)
        }

        def hasVariables: Boolean = {
            hasUserVariables || hasGlobalVariables
        }

        def hasUserVariables: Boolean = {
            USER_VARIABLE.r.test(sentence)
        }

        def hasGlobalVariables: Boolean = {
            GLOBAL_VARIABLE.r.test(sentence)
        }

        def matchParameters: List[Match] = {
            PARAMETER.r.findAllMatchIn(sentence).toList
        }

        //匹配的几个问题
        //先替换长字符匹配，再替换短字符匹配，如 #user 和 #username, 应先替换 #username，再替换 #user
        //原生特殊字符处理，如输出#，则使用两个重复的##

        //适用于DataHub pass和put的方法, 对应DataSource的 tableSelect和tableUpdate
        def replaceParameters(row: DataRow): String = {

            PARAMETER
                    .r
                    .findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(4))
                    .reverse
                    .foreach(m => {

                        val whole = m.group(0)
                        val fieldName = m.group(4)
                        val symbol = m.group(3)
                        val prefix = m.group(1) //前缀
                        val suffix = if (!m.group(2).contains("(") && m.group(2).contains(")")) ")" else ""

                        if (symbol == "#") {
                            if (row.contains(fieldName)) {
                                sentence = sentence.replace(whole, prefix + row.getString(fieldName) + suffix)
                            }
                        }
                        else if (symbol == "&") {
                            if (row.contains(fieldName)) {
                                sentence = sentence.replace(whole, (row.getDataType(fieldName), row.get(fieldName)) match {
                                    case (Some(dataType), Some(value)) =>
                                        if (value == null) {
                                            prefix + "NULL" + suffix
                                        }
                                        else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                            prefix + value.toString + suffix
                                        }
                                        else {
                                            prefix + "'" + value.toString.replace("'", "''") + "'" + suffix
                                        }
                                    case _ => prefix
                                })
                            }
                        }
                    })

            sentence = sentence.replace("##", "#")
                                .replace("&&", "&")

            sentence
        }

        //处理语句中的js表达式, 目前不判断类型即无保留双引号的情况
        def replaceJsExpressions(retainQuotes: Boolean = false): String = {
            JS_EXPRESSION
                    .r
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        m.group(1).eval()
                            .ifNotNull(cell => {
                                sentence = sentence.replace(m.group(0), cell.toString.useQuotesIf(retainQuotes && (cell.dataType == DataType.TEXT || cell.dataType == DataType.DATETIME)))
                            })
                            .ifNull(() => throw new SQLExecuteException(s"{ Wrong js expression: ${m.group(0)}}"))
                    })

            sentence
        }

        //处理语句中的js语句块, 目前不判断类型即无保留双引号的情况
        def replaceJsStatements(retainQuotes: Boolean = false): String = {
            JS_STATEMENT
                    .r
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        m.group(1).call()
                            .ifNotNull(cell =>
                                sentence = sentence.replace(m.group(0),
                                    cell.value.toString.useQuotesIf(retainQuotes && (cell.dataType == DataType.TEXT || cell.dataType == DataType.DATETIME))
                                )
                            )
                            .ifNull(() => throw new SQLExecuteException(s"{ Wrong js statement: ${m.group(0)}}"))
                    })

            sentence
        }

        //解析表达式中的变量
        def replaceUserVariables(PSQL: PSQL, retainQuotes: Boolean = false): String = {

            USER_VARIABLE
                    .r
                    .findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(3))
                    .reverse
                    .foreach(m => {
                        val whole = m.group(0)
                        val fieldName = m.group(2)
                        val prefix = whole.takeBefore("$")

                        val fieldValue = PSQL.findVariableValue(fieldName)
                        if (fieldValue.isNotNull) {
                            sentence = sentence.replace(whole,
                                prefix + fieldValue.value.toString.useQuotesIf(retainQuotes && fieldValue.dataType == DataType.TEXT))
                        }
                    })

            sentence = sentence.replace("$$", "$")
            sentence
        }

        def replaceGlobalVariables(PSQL: PSQL, retainQuotes: Boolean = false): String = {
            GLOBAL_VARIABLE
                    .r.
                    findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(3))
                    .reverse
                    .foreach(m => {
                        val whole = m.group(0)
                        val fieldName = m.group(2)
                        val prefix = whole.takeBefore("@")

                        val fieldValue = GlobalVariable.get(fieldName, PSQL)
                        if (fieldValue.isNotNull) {
                            sentence = sentence.replace(whole, prefix + fieldValue.toString.useQuotesIf(retainQuotes && (fieldValue.dataType == DataType.TEXT || fieldValue.dataType == DataType.DATETIME)))
                        }
                    })

            sentence = sentence.replace("@@", "@")
            sentence
        }

        def `##->#`: String = {
            sentence = sentence.replace("##", "#")
            sentence
        }

        def `&&->&`: String = {
            sentence = sentence.replace("&&", "&")
            sentence
        }
        def `$$->$`: String = {
            sentence = sentence.replace("$$", "$")
            sentence
        }

        //解析完整的表达式 - SET表达式右侧 / 条件表达式左右侧 / 函数的每个参数 / FOR-IN 表达式右侧 / FOR-TO 表达式左右侧
        //解析过程中，字符串需要保留双引号

        // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        //var parsed = parseVariables(expression, true)
        // 函数 ${FUNC()} $FUNC()
        //parsed = parseFunctions(parsed, true)

        //解析表达式中的函数
        def replaceSystemFunctions(retainQuotes: Boolean): String = {

            val p = Pattern.compile(SYSTEM_FUNCTION, Pattern.CASE_INSENSITIVE)
            var m: Matcher = null
            var chr: String = null
            var start: Int = 0
            var end: Int = 0

            while ( {m = p.matcher(sentence); m}.find(start)) {
                val brackets = new mutable.ArrayStack[String]()
                brackets.push("(")
                start = sentence.indexOf(m.group)
                end = start + m.group.length
                breakable {
                    while (brackets.nonEmpty) {
                        chr = sentence.substring(end, end + 1)
                        chr match {
                            case "(" =>
                                if (brackets.last != "\"") {
                                    brackets.push("(")
                                }
                            case ")" =>
                                if (brackets.last != "\"") {
                                    brackets.pop()
                                }
                            case "\"" =>
                                if (brackets.last == "\"") {
                                    brackets.pop()
                                }
                                else {
                                    brackets.push("\"")
                                }
                        }
                        //遇到转义符，前进一位
                        end += (if (chr == "\\") 2 else 1)
                        if (end >= sentence.length) {
                            break
                        }
                    }
                }
                //函数应该正确闭合
                if (brackets.nonEmpty) {
                    brackets.clear()
                    throw new SQLExecuteException("Miss right bracket in internal function: " + m.group(0))
                }
                //取按开始点和结束点正确的函数
                val function: String = sentence.substring(start, end)
                var replacement: String = Function(function).call()
                if (!retainQuotes) {
                    replacement = replacement.removeQuotes()
                }
                start += replacement.length
                sentence = sentence.replace(function, replacement)
            }

            sentence
        }

        def replacePlaceHolders(PSQL: PSQL, retainQuotes: Boolean = false): String = {
            sentence = sentence
                    .replaceGlobalVariables(PSQL, retainQuotes)
                    .replaceUserVariables(PSQL, retainQuotes)
                    .replaceSystemFunctions(retainQuotes)
                    .replaceJsExpressions()
                    .replaceJsStatements()

            sentence
        }

        def $place(PSQL: PSQL): String = {
            sentence.replacePlaceHolders(PSQL, false)
        }

        def parseExpression(PSQL: PSQL, retainQuotes: Boolean = true): String = {
            sentence = sentence.replacePlaceHolders(PSQL, true)
            sentence.eval()
                    .ifNotNull(cell => {
                        //如果是字符串，则加上引号
                        sentence = cell.value.toString
                        if (retainQuotes && (cell.dataType == DataType.TEXT || cell.dataType == DataType.DATETIME)) {
                            sentence = sentence.useQuotes()
                        }
                    })
                    .ifNull(() => throw new SQLExecuteException("Can't calculate the expression: " + sentence))

            sentence
        }

        def $eval(PSQL: PSQL): String = {
            sentence.parseExpression(PSQL, false)
        }

        def evalExpression(PSQL: PSQL): DataCell = {
            sentence.replacePlaceHolders(PSQL, true).eval()
        }
    }
}