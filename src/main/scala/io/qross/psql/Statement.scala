package io.qross.psql

import java.util.regex.Pattern

import io.qross.core.{DataCell, DataRow}
import io.qross.util.{Console, Output}

import scala.collection.mutable.ArrayBuffer

object Statement {
    val $EXPRESSION: Pattern = Pattern.compile("""\$\{\{(.+?)}}""", Pattern.CASE_INSENSITIVE)
    val $FUNCTION: Pattern = Pattern.compile("\\$\\{?(" + String.join("|", Function.NAMES) + ")\\s*\\(", Pattern.CASE_INSENSITIVE)
    val $VARIABLE: Pattern = Pattern.compile("""\$\{?([a-z_][a-z0-9_]*)}?""", Pattern.CASE_INSENSITIVE)

    //为计算值添加双引号，用于计算过程中
    def useQuotes(value: String): String = "\"" + value.replace("\"", "\\\"") + "\""

    //去掉常量中的双引号，用于计算结果
    def removeQuotes(value: String): String = {
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
            value.substring(1, value.length - 1).replace("\\\"", "\"")
        }
        else {
            value
        }
    }

    def removeVariableModifier(value: String): String = value.replace("$", "").replace("{", "").replace("}", "").trim
}

class Statement(PSQL: PSQL, caption: String, sentence: String, expressions: String*) {

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
        sentence = parseExpressions(sentence)
        // 之前决定的是先解析函数再解析变量，忘了为啥了 2018.12.7
        // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        sentence = parseVariables(sentence, false)
        // 函数 ${FUNC()} $FUNC()
        sentence = parseFunctions(sentence, false)
        sentence
    }
}
