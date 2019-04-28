package io.qross.psql

import java.util.regex.Matcher

import io.qross.jdbc.DataSource
import io.qross.ext.Output
import io.qross.psql.Patterns._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ConditionGroup(statement: Statement, expression: String) {

    private val conditions = new ArrayBuffer[Condition]()
    private val ins = new ArrayBuffer[String]()
    private val exists = new ArrayBuffer[String]()
    private val selects = new ArrayBuffer[String]()

    def evalAll(ds: DataSource): Boolean = {

        //解析表达式
        var exp = statement.parseExpressions(expression)

        //replace SELECT to #[select:n]
        var m = $_SELECT.matcher(exp)
        while (m.find) {
            val select = findOutSelect(exp, m.group)
            exp = exp.replace(select, "#[select:" + selects.size + "]")
            selects += select
        }
        //replace EXISTS () to #[exists:n]
        m = $EXISTS.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(1), "#[exists:" + exists.size + "]")
            exists += m.group(1)
        }

        //replace IN () to #[in:n]
        m = $IN.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(1), "#[in:" + ins.size + "]")
            ins += m.group(1)
        }

        //解析变量和函数
        exp = statement.parseVariablesAndFunctions(exp, true)

        // ()
        while ({m = $BRACKET.matcher(exp); m}.find) {
            parseBasicExpression(m.group(1).trim)
            exp = exp.replace(m.group(0), CONDITION + (this.conditions.size - 1) + N)
        }

        //finally
        parseBasicExpression(exp)

        //IN (SELECT ...)
        val selectResult = new ArrayBuffer[String]
        for (select <- selects) {
            selectResult += ds.executeSingleList(this.statement.parseVariablesAndFunctions(select, false)).mkString(",")
        }

        for (condition <- this.conditions) {
            var field = condition.field
            var value = condition.value

            m = SELECT$.matcher(value)
            while (m.find) {
                value = value.replace(m.group(0), selectResult(m.group(1).toInt))
            }

            if ({m = CONDITION$.matcher(field); m}.find) {
                field = conditions(m.group(1).toInt).result.toString
            }
            else if (field.nonEmpty) {
                field = statement.parseSingleExpression(field, false)
            }

            if ({m = CONDITION$.matcher(value); m}.find) {
                value = conditions(m.group(1).toInt).result.toString
            }
            else if (!value.equalsIgnoreCase("EMPTY") && !value.equalsIgnoreCase("NULL") && value != "()") {
                value = statement.parseSingleExpression(value, false)
            }

            condition.eval(field, value)

            Output.writeDotLine(" ", condition.field, condition.operator, condition.value, " => ", condition.result)
        }

        conditions(conditions.size - 1).result
    }

    private def findOutSelect(expression: String, head: String): String = {
        var start: Int = 0
        val begin: Int = expression.indexOf(head, start) + 1
        var end: Int = expression.indexOf(")", start)

        val brackets = new mutable.ArrayStack[String]
        brackets.push("(")
        start = begin

        while (brackets.nonEmpty && expression.indexOf(")", start) > - 1) {
            val left: Int = expression.indexOf("(", start)
            val right: Int = expression.indexOf(")", start)
            if (left > -1 && left < right) {
                brackets.push("(")
                start = left + 1
            }
            else {
                brackets.pop()
                start = right + 1
                if (right > end) end = right
            }
        }

        if (brackets.nonEmpty) {
            throw new SQLParserException("Can't find closed bracket for SELECT: " + expression)
        }
        else {
            expression.substring(begin, end)
        }
    }

    //解析无括号的表达式
    def parseBasicExpression(expression: String): Unit = {
        var m: Matcher = null
        var n: Matcher = null
        var left = ""
        var right = ""
        var clause = ""
        var exp = expression

        //restore EXISTS
        m = EXISTS$.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(0), exists(m.group(1).toInt))
        }

        //restore IN
        m = IN$.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(0), ins(m.group(1).toInt))
        }

        //AND
        while ({m = $AND.matcher(exp); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            while ({n = $_OR.matcher(clause); n}.find) {
                clause = clause.substring(clause.indexOf(n.group) + n.group.length)
                left = left.substring(left.indexOf(n.group) + n.group.length)
            }

            if (!left.startsWith(CONDITION)) {
                exp = exp.replace(left, CONDITION + this.conditions.size + N)
                clause = clause.replace(left, CONDITION + this.conditions.size + N)
                conditions += new Condition(left.trim)
            }

            if (!right.startsWith(CONDITION)) {
                exp = exp.replace(right, CONDITION + this.conditions.size + N)
                clause = clause.replace(right, CONDITION + this.conditions.size + N)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, CONDITION + this.conditions.size + N)
            conditions += new Condition(clause.trim) // left AND right

        }
        //OR
        while ({m = $OR.matcher(expression); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            if (!left.startsWith(CONDITION)) {
                exp = exp.replace(left, CONDITION + this.conditions.size + N)
                clause = clause.replace(left, CONDITION + this.conditions.size + N)
                conditions += new Condition(left.trim)
            }

            if (!right.startsWith(CONDITION)) {
                exp = exp.replace(right, CONDITION + this.conditions.size + N)
                clause = clause.replace(right, CONDITION + this.conditions.size + N)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, CONDITION + this.conditions.size + N)
            this.conditions += new Condition(clause.trim) // left OR right

        }

        //SINGLE
        if (!expression.startsWith(CONDITION)) {
            conditions += new Condition(expression.trim)
        }
    }
}
