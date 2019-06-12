package io.qross.worker

import io.qross.core.DataHub
import io.qross.ext.PlaceHolder._
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._
import io.qross.fs.ResourceFile
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.Properties
import io.qross.sql.SQLExecuteException

import scala.collection.mutable
import scala.io.Source

object Worker {

    def main(args: Array[String]): Unit = {

        var SQL: String = "" //待执行的PSQL语句

        for (i <- args.indices) {
            if (args(i).startsWith("--") && args.length > i + 1) {
                args(i).toLowerCase() match {
                    case "--file" => //从文件中加载SQL语句
                        SQL = Source.fromFile(args(i+1).locate()).mkString
                    case "--sql" => //执行SQL语句, 不支持在语句中使用双引号，双引号用~u0034代替
                        SQL = args(i+1).replace("~u0034", "\"")
                    case "--resources" =>
                        SQL = ResourceFile.open(args(i+1)).getContent()
                    case "--vars" => //传递参数
                        SQL = SQL.replaceArguments(args(i+1).toHashMap())
                    case "--properties" => //加载properties文件
                        Properties.loadLocalFile(args(i+1).locate())
                    case "--jdbc" => //加载数据源
                        args(i+1).toHashMap().foreach(item => {
                            Properties.set(item._1, item._2)
                        })
                    case "--note" => //执行Note
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.querySingleValue("SELECT psql FROM qross_notes WHERE id=?", args(i)).getOrElse("")
                        }
                    case "--event" => //执行Keeper事件
                        if (JDBC.hasQrossSystem) {
                            SQL = DataSource.querySingleValue("SELECT event_value FROM qross_jobs_events WHERE id=?", args(i)).getOrElse("")
                        }
                    case _ =>
                }
            }
        }

        if (SQL != "") {
            val dh = new DataHub()

            dh.run(SQL)

            dh.close()
        }
        else {
            throw new SQLExecuteException("No PSQL to execute.")
        }
    }
}
