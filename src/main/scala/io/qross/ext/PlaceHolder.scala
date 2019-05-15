package io.qross.ext

import io.qross.core.{DataCell, DataRow}
import io.qross.jdbc.DataType

import scala.collection.mutable
import scala.util.matching.Regex.Match
import io.qross.ext.TypeExt._
import io.qross.psql.SQLExecuteException

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
    def ARGUMENTS: PlaceHolder = new PlaceHolder("""[^#&](([#&])\{([a-zA-Z_][a-zA-Z0-9_]+)\})""")   //入参 #{name} 或 &{name}
    def PARAMETERS: PlaceHolder = new PlaceHolder("""[^#&](([#&])\(?([a-zA-Z_][a-zA-Z0-9_]+)\)?)""") //DataHub传递参数, #name 或 #{name} 或 &name 或 &(name)
    def USER_VARIABLES: PlaceHolder = new PlaceHolder("""\$\(?([a-zA-Z_][a-zA-Z0-9_]+)\)?""") //用户变量
    def USER_DEFINED_FUNCTIONS: PlaceHolder = new PlaceHolder("""$[a-zA-Z_]+\(\)""") //未完成, 用户函数
    def SHARP_EXPRESSIONS: PlaceHolder = new PlaceHolder(s"""${}""") //未完成, Sharp表达式
    def GLOBAL_VARIABLES: PlaceHolder = new PlaceHolder("""@\(?([a-zA-Z_][a-zA-Z0-9_]+)\)?""") //全局变量
    def SYSTEM_FUNCTIONS: PlaceHolder = new PlaceHolder("@func") //未完成, 系统函数
    def JS_EXPRESSIONS: PlaceHolder = new PlaceHolder("""\~\{(.+?)}""", """\~\{\{(.+?)}}""") //js表达式和js语句块
}

class PlaceHolder(regex: String*) {

    private val matches = new mutable.ArrayBuffer[Match]()
    private var sentence = ""

    def in(sentence: String): PlaceHolder = {
        this.sentence = sentence
        regex.foreach(rex => {
            matches ++= rex.r.findAllMatchIn(sentence)
        })

        this
    }

    def matched: Boolean = matches.nonEmpty

    def first: Option[String] = {
        if (matched) {
            Some(matches.head.group(1))
        }
        else {
            None
        }
    }

    def all: List[String] = {
        matches.map(m => m.group(1)).toList
    }

    //匹配的几个问题
    //先替换长字符匹配，再替换短字符匹配，如 #user 和 #username, 应先替换 #username，再替换 #user
    //原生特殊字符处理，如输出#，则使用两个重复的##

    //适用于DataHub pass和put的方法, 对应DataSource的 tableSelect和tableUpdate
    def replaceWith(row: DataRow): String = {

        var replacement = this.sentence

        matches.sortBy(m => m.group(1)).reverse.foreach(m => {

            val whole = m.group(0)
            val fieldName = m.group(3)
            val symbol = m.group(2)
            val prefix = whole.takeBefore(symbol) //前缀

            if (symbol == "#") {
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(whole, prefix + row.getString(fieldName))
                }
            }
            else if (symbol == "&") {
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(whole, (row.getDataType(fieldName), row.get(fieldName)) match {
                        case (Some(dataType), Some(value)) =>
                            if (value == null) {
                                prefix + "NULL"
                            }
                            else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                prefix + value.toString
                            }
                            else {
                                prefix + "'" + value.toString.replace("'", "''") + "'"
                            }
                        case _ => prefix
                    })
                }
            }
        })

        replacement = replacement.replace("##", "#")
        replacement = replacement.replace("&&", "&")

        replacement
    }

    //适用于js表达式和语句块
    def eval(retainQuotes: Boolean = false): DataCell = {
        var replacement = this.sentence

        matches.foreach(m => {
            { if (m.group(0).startsWith("~{{")) m.group(1).call() else m.group(1).eval() } match {
                case Some(data) =>
                    replacement = replacement.replace(m.group(0),
                                        if (retainQuotes && data.dataType == DataType.TEXT) {
                                            data.value.toString.useQuotes()
                                        }
                                        else {
                                            data.value.toString
                                        })
                case None => throw new SQLExecuteException(s"{ Wrong js expression or statement: ${m.group(0)}}")
            }
        })

        //replacement
        new DataCell("")
    }
}
