package io.qross.psql

import java.util.regex.{Matcher, Pattern}

import io.qross.core.{DataCell, DataRow}
import io.qross.jdbc.DataType
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.psql.Patterns._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class Statement(val PSQL: PSQL, var caption: String, var sentence: String, expressions: String*) {

    var instance: Any = _
    //表示控制语句是否闭合, 用于解析检查
    var closed: Boolean = true
    //所有子语句
    val statements = new ArrayBuffer[Statement]()

    //局部变量列表，对于root，则表示全局变量
    private val variables = new DataRow()

    caption match {
        case "ROOT" =>            
        case "IF" =>
            this.instance = new IfElse(this, expressions(0))
        case "ELSE_IF" =>
            this.instance = new IfElse(this, expressions(0))
        case "ELSE" =>
        case "END_IF" =>
        case "FOR_SELECT" =>
            this.instance = new ForSelectLoop(this, expressions(0), expressions(1))
        case "FOR_IN" =>
            this.instance = new ForInLoop(this, expressions(0), expressions(1), expressions(2))
        case "FOR_TO" =>
            this.instance = new ForToLoop(this, expressions(0), expressions(1), expressions(2))
        case "WHILE" =>
            this.instance = new WhileLoop(this, expressions(0))
        case "END_LOOP" =>
        case "SET" =>
            this.instance = new SetVariable(this, expressions(0), expressions(1))
        case _ =>
            this.instance = new QuerySentence(this, expressions(0), expressions(1), expressions(2), expressions(3))
    }

    def containsVariable(name: String): Boolean = this.variables.contains(name)

    def getVariable(name: String): DataCell = this.variables.getCell(name)

    def setVariable(name: String, value: Any): Unit = this.variables.set(name, value)

    def addStatement(statement: Statement): Unit = this.statements += statement

    def show(level: Int): Unit = {
        for (i <- 0 until level) {
            System.out.print("\t")
        }
        Output.writeLine(this.sentence)
        for (statement <- this.statements) {
            statement.show(level + 1)
        }
    }

    //不能事先把变量替换成数值，因为可能之后这个变量的值可能会发生变化
    //在执行时才计算变量的值
    //解析查询语句中的表达式、函数和变量，均不保留双引号
    def parseQuerySentence(sentence: String): String = { //均不保留双引号
        // 表达式 ${{ expression }}
        var parsed = parseExpressions(sentence)
        // 之前决定的是先解析函数再解析变量，忘了为啥了 2018.12.7
        // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        parsed = parseVariables(parsed, false)
        // 函数 ${FUNC()} $FUNC()
        parseFunctions(parsed, false)
    }

    //解析标准表达式语句
    def parseStandardSentence(sentence: String): String = {
        val parsed = parseExpressions(sentence)
        parseSingleExpression(parsed, false)
    }

    //解析条件表达式语句
    def parseVariablesAndFunctions(sentence: String, retainQuotes: Boolean = false): String = {
        val parsed = parseVariables(sentence, retainQuotes)
        parseFunctions(parsed, retainQuotes)
    }

    //解析查询语句中的表达式
    def parseExpressions(sentence: String): String = {
        var parsed = sentence
        val m = $EXPRESSION.matcher(parsed)
        while (m.find) {
            parsed = parsed.replace(m.group(0), parseSingleExpression(m.group(1), false))
        }
        parsed
    }

    //解析完整的表达式 - SET表达式右侧 / 条件表达式左右侧 / 函数的每个参数 / FOR-IN 表达式右侧 / FOR-TO 表达式左右侧
    //解析过程中，字符串需要保留双引号
    def parseSingleExpression(expression: String, retainQuotes: Boolean): String = { // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        var parsed = parseVariables(expression, true)
        // 函数 ${FUNC()} $FUNC()
        parsed = parseFunctions(parsed, true)
        // 以js方式执行表达式
        parsed.eval() match {
            case Some(data) =>
                parsed = data.value.toString
                //如果是字符串，则加上引号
                if (retainQuotes && data.dataType == DataType.TEXT) {
                    parsed = parsed.useQuotes()
                }
            case None => throw new SQLParserException("Can't calculate the expression: " + parsed)
        }

        parsed
    }

    //解析表达式中的函数
    def parseFunctions(expression: String, retainQuotes: Boolean): String = {
        var m: Matcher = null
        var chr: String = null
        var start: Int = 0
        var end: Int = 0
        var parsed = expression
        while ( {m = $FUNCTION.matcher(parsed); m}.find(start)) {
            val brackets = new mutable.ArrayStack[String]()
            brackets.push("(")
            start = parsed.indexOf(m.group)
            end = start + m.group.length
            breakable {
                while (brackets.nonEmpty) {
                    chr = parsed.substring(end, end + 1)
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
                    if (end >= parsed.length) {
                        break
                    }
                }
            }
            //函数应该正确闭合
            if (brackets.nonEmpty) {
                brackets.clear()
                throw new SQLParserException("Miss right bracket in internal function: " + m.group(0))
            }
            //取按开始点和结束点正确的函数
            val function: String = parsed.substring(start, end)
            var replacement: String = new Function(this).execute(function)
            if (!retainQuotes) {
                replacement = replacement.removeQuotes()
            }
            start += replacement.length
            parsed = parsed.replace(function, replacement)
        }

        parsed
    }

    //解析表达式中的变量
    def parseVariables(expression: String, retainQuotes: Boolean): String = {
        var worked = ""
        var parsed = expression
        val m = $VARIABLE.matcher(parsed)
        while (m.find) {
            worked += parsed.substring(0, parsed.indexOf(m.group))
            parsed = parsed.substring(parsed.indexOf(m.group) + m.group.length)

            val result = this.PSQL.findVariableValue(m.group(1))
            var replacement: String = null
            if (result != null) {
                replacement = result.value.toString
                if (retainQuotes && result.dataType == DataType.TEXT) {
                    replacement = replacement.useQuotes()
                }
            }
            if (replacement != null) {
                worked += replacement
            }
            else {
                worked += m.group
            }
        }

        worked + parsed
    }
}
