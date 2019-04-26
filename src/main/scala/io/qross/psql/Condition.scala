package io.qross.psql

import java.util.regex.{Matcher, Pattern}

import io.qross.jdbc.DataType
import io.qross.util.TypeExt._

class Condition(var expression: String) {
    var field: String = null
    var operator: String = null
    var value: String = null //or unary

    var result = false

    /*
    operator 列表

    等于 =, ==
    不等于 !=, <>
    开始于 ^=
    非开始于 =^
    结束于 $=
    非结束于 =$
    包含于 *=
    不包含于 =*
    正则表达式匹配 #=
    正则表达式不匹配 =#
    存在 EXISTS ()
    不存在 NOT EXISTS ()
    在列表中 IN ()
    不在列表中 NOT IN ()
    大于 >
    小于 <
    大于等于 >=
    小于等于 <=
    为NULL IS NULL
    不为NULL IS NOT NULL
    为空值 IS EMPTY
    不为空值 IS NOT EMPTY

    AND
    OR
    NOT
    */

    private val $OPERATOR = Pattern.compile("==|!=|<>|>=|<=|\\^=|=\\^|\\$=|=\\$|\\*=|=\\*|#=|=#|>|<|=|\\sNOT\\s+IN\\s|\\sIS\\s+NOT\\s|\\sIN\\s|\\sIS\\s|\\sAND\\s|\\sOR\\s|^NOT\\s+EXISTS|^EXISTS|^NOT\\s", Pattern.CASE_INSENSITIVE)
    private val $IS = Pattern.compile("^IS\\s", Pattern.CASE_INSENSITIVE)
    private val $BOOLEANS = Map[String, Boolean](
        "true" -> true,
        "yes" -> true,
        "ok" -> true,
        "1" -> true,
        "on" -> true,
        "false" -> false,
        "no" -> false,
        "0" -> false,
        "cancel" -> false,
        "off" -> false
    )


    if ($IS.matcher(expression).find) {
        expression = " " + expression
    }
    private val m: Matcher = $OPERATOR.matcher(expression)
    if (m.find) {
        this.operator = m.group
        this.field = expression.substring(0, expression.indexOf(this.operator)).trim
        this.value = expression.substring(expression.indexOf(this.operator) + this.operator.length).trim
        this.operator = this.operator.trim.toUpperCase
        this.operator = Pattern.compile("\\s").matcher(this.operator).replaceAll("")
    }
    else {
        this.operator = "" //default
        //throw new SQLParserException("Wrong condition clause: " + expression);
    }

    private def parseBoolean(bool: String): Boolean = {
        $BOOLEANS.getOrElse(bool.trim.toLowerCase.removeQuotes(), false)
    }

    def eval(field: String, value: String): Unit = {
        this.operator match {
            case "AND" => this.result = parseBoolean(field) && parseBoolean(value)
            case "OR" => this.result = parseBoolean(field) || parseBoolean(value)
            case "NOT" => this.result = !parseBoolean(value)
            case "EXISTS" => this.result = value != "()"
            case "NOTEXISTS" => this.result = value == "()"
            case "IN" =>
                this.result = value.$trim("(", ")")
                                     .split(",")
                                     .map(_.trim)
                                     .contains(
                                         if (value.contains("'")) {
                                             field.$quote("'")
                                         }
                                         else if (value.contains("\"")) {
                                             field.$quote("\"")
                                         }
                                         else {
                                             field
                                         }
                                     )
            case "NOTIN" =>
                this.result = !value.$trim("(", ")")
                                    .split(",")
                                    .map(_.trim)
                                    .contains(
                                        if (value.contains("'")) {
                                            field.$quote("'")
                                        }
                                        else if (value.contains("\"")) {
                                            field.$quote("\"")
                                        }
                                        else {
                                            field
                                        }
                                    )
            case "IS" =>
                if (value.equalsIgnoreCase("NULL")) {
                    this.result = (field.contains("#{") || field.contains("${")) && field.contains("}") || field.equalsIgnoreCase("NULL")
                }
                else if (value.equalsIgnoreCase("EMPTY")) {
                    this.result = field.isEmpty || field == "\"\"" || field == "''"
                }

            case "ISNOT" =>
                if (value.equalsIgnoreCase("NULL")) {
                    this.result = !field.contains("#{") && !field.contains("${") && !field.equalsIgnoreCase("NULL")
                }
                else if (value.equalsIgnoreCase("EMPTY")) {
                    this.result = field.nonEmpty && field != "\"\"" && field != "''"
                }
            case "=" =>
            case "==" =>
                this.result = field.equalsIgnoreCase(value)
            case "!=" =>
            case "<>" =>
                this.result = !field.equalsIgnoreCase(value)
            case "^=" =>
                this.result = field.toLowerCase.startsWith(value.toLowerCase)
            case "=^" =>
                this.result = !field.toLowerCase.startsWith(value.toLowerCase)
            case "$=" =>
                this.result = field.toLowerCase.endsWith(value.toLowerCase)
            case "=$" =>
                this.result = !field.toLowerCase.endsWith(value.toLowerCase)
            case "*=" =>
                this.result = field.toLowerCase.contains(value.toLowerCase)
            case "=*" =>
                this.result = !field.toLowerCase.contains(value.toLowerCase)
            case "#=" =>
                this.result = Pattern.compile(value, Pattern.CASE_INSENSITIVE).matcher(field).find
            case "=#" =>
                this.result = !Pattern.compile(value, Pattern.CASE_INSENSITIVE).matcher(field).find
            case ">=" =>
                try
                    this.result = field.toDouble >= value.toDouble
                catch {
                    case e: Exception =>
                        throw new SQLParserException("Value must be number on >= compare: " + field + " >= " + value)
                }
            case "<=" =>
                try
                    this.result = field.toDouble <= value.toDouble
                catch {
                    case e: Exception =>
                        throw new SQLParserException("Value must be number on >= compare: " + field + " <= " + value)
                }
            case ">" =>
                try
                    this.result = field.toDouble > value.toDouble
                catch {
                    case e: Exception =>
                        throw new SQLParserException("Value must be number on >= compare: " + field + " > " + value)
                }
            case "<" =>
                try
                    this.result = field.toDouble < value.toDouble
                catch {
                    case e: Exception =>
                        throw new SQLParserException("Value must be number on >= compare: " + field + " < " + value)
                }
            case _ =>
                this.expression.eval() match {
                    case Some(data) => this.result = parseBoolean(data.value.toString)
                    case None => this.result = parseBoolean(this.expression)
                }
        }
    }
}
