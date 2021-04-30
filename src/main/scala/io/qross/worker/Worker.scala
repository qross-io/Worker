package io.qross.worker

import java.util

import io.qross.core.DataHub
import io.qross.exception.SQLExecuteException
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.FileReader
import io.qross.fs.Path._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.PQL
import io.qross.setting.Properties

object Worker {

    def main(args: Array[String]): Unit = {

        var SQL: String = "" //待执行的PQL语句
        var debug = false
        var log = "text"
        var vars: String = ""
        var userId: Int = 0
        var userName: String = ""
        var role: String = "worker"
        val info: java.util.Map[String, Any] = new util.HashMap[String, Any]()

        var url = ""
        var driver = ""
        var username = ""
        var password = ""

        for (i <- args.indices) {
            if (args(i).startsWith("-") && args.length > i + 1) {
                args(i).toLowerCase() match {
                    case "-file" => //从文件中加载SQL语句
                        //SQL = Source.fromFile(args(i+1).locate(), "UTF-8").mkString
                        var file = args(i+1)
                        if (file.contains("?")) {
                            vars = file.takeAfter("?")
                            file = file.takeBefore("?")
                        }
                        SQL = new FileReader(file.locate()).readToEnd(!_.startsWith("--"))
                    case "-debug" =>
                        debug = args(i+1).toBoolean(false)
                    case "-log" => //log format
                        log = args(i+1).toLowerCase()
                    case "-sql" => //执行SQL语句, 不支持在语句中使用双引号，双引号用~u0034代替
                        SQL = args(i+1).replace("~u0034", "\"")
                    case "-vars" | "--args" => //传递参数
                        //参数支持PQL所有嵌入规则
                        vars = args(i+1)
                    case "-properties" => //加载properties文件
                        Properties.loadLocalFile(args(i+1).locate())
                    case "-note" => //执行Note
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.QROSS.querySingleValue("SELECT note_code FROM qross_notes WHERE id=?", args(i+1)).asText("")
                        }
                    case "-task" => //执行Keeper任务
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.QROSS.querySingleValue("SELECT command_text FROM qross_tasks_dags WHERE id=?", args(i+1)).asText("")
                        }
                    case "-login" | "--signin" =>
                        args(i+1).$split().foreach(item => {
                            if (Set[String]("id", "userid", "uid", "user").contains(item._1.toLowerCase)) {
                                userId = item._2.toInt
                            }
                            else if (Set[String]("name", "username").contains(item._1.toLowerCase)) {
                                userName = item._2
                            }
                            else if (Set[String]("role", "rolename").contains(item._1.toLowerCase)) {
                                role = item._2.toLowerCase()
                            }
                            else {
                                info.put(item._1, item._2)
                            }
                        })
                    case "-driver" =>
                        driver = args(i+1)
                    case "-url" =>
                        url = args(i+1)
                    case "-username" =>
                        username = args(i+1)
                    case "-password" =>
                        password = args(i+1)
                    case _ =>
                }
            }
        }

        if (SQL != "") {
            //按PQL计算, 支持各种PQL嵌入式表达式, 但不保留引号
            new PQL(SQL, DataHub.DEFAULT.debug(debug, log))
                .signIn(userId, userName, role, info)
                .place(vars)
                .run()
        }
        else if (url != "") {
            Output.writeLine(DataSource.testConnection(driver, url, username, password))
        }
        else {
            throw new SQLExecuteException("No PQL sentences to execute.")
        }
    }
}