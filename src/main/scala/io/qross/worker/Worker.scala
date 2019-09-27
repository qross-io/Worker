package io.qross.worker

import io.qross.core.DataHub
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._
import io.qross.fs.ResourceFile
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.Properties
import io.qross.pql.SQLExecuteException
import io.qross.core.Authentication._
import io.qross.pql.PQL._

import scala.io.Source

object Worker {

    def main(args: Array[String]): Unit = {

        var SQL: String = "" //待执行的PSQL语句
        var vars: String = ""
        var userId: Int = 0
        var userName: String = ""

        for (i <- args.indices) {
            if (args(i).startsWith("--") && args.length > i + 1) {
                args(i).toLowerCase() match {
                    case "--file" => //从文件中加载SQL语句
                        SQL = Source.fromFile(args(i+1).locate()).mkString
                    case "--sql" => //执行SQL语句, 不支持在语句中使用双引号，双引号用~u0034代替
                        SQL = args(i+1).replace("~u0034", "\"")
                    case "--resources" =>
                        SQL = ResourceFile.open(args(i+1)).content
                    case "--vars" => //传递参数
                        //在传递给worker之前必须事先都计算好
                        vars = args(i+1)
                    case "--properties" => //加载properties文件
                        Properties.loadLocalFile(args(i+1).locate())
                    case "--jdbc" => //加载数据源
                        args(i+1).toHashMap().foreach(item => {
                            Properties.set(item._1, item._2)
                        })
                    case "--note" => //执行Note
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.QROSS.querySingleValue("SELECT pql FROM qross_notes WHERE id=?", args(i+1)).asText
                        }
                    case "--task" => //执行Keeper任务
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.QROSS.querySingleValue("SELECT command_text FROM qross_tasks_dags WHERE id=?", args(i+1)).asText
                        }
                    case "--login" =>
                        args(i+1).toHashMap().foreach(item => {
                            if (Set[String]("id", "userid", "uid", "user").contains(item._1.toLowerCase)) {
                                userId = item._2.toInt
                            }
                            else if (Set[String]("name", "username").contains(item._1.toLowerCase)) {
                                userName = item._2
                            }
                        })
                    case _ =>
                }
            }
        }

        if (SQL != "") {
            val dh = new DataHub()

            //按PQL计算, 支持各种PQL嵌入式表达式, 但不保留引号

            dh.signIn(userId, userName)
                .setArgs(vars)
                    .run(SQL)

            dh.close()
        }
        else {
            throw new SQLExecuteException("No PQL sentences to execute.")
        }
    }
}