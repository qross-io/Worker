package io.qross.psql

import io.qross.core.{DataCell, DataRow}
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.{Configurations, Global, Properties}
import io.qross.time.DateTime

object GlobalVariable {

     val GLOBALS: Set[String] = Global.getClass.getDeclaredMethods.map(_.getName).toSet
    //除环境全局变量的其他全局变量
    val SYSTEM: DataRow = new DataRow()
    val USER: DataRow = new DataRow()
    val PROGRESS: DataRow = new DataRow()

    //Global.getClass.getDeclaredMethods.contains()
    //Global.getClass.getMethod("").invoke(null)

    //从数据库加载全局变量
    if (JDBC.hasQrossSystem) {
        // USER:NAME -> VALUE
        DataSource.queryDataTable("SELECT var_name, var_type, var_value FROM qross_variables WHERE var_group='SYSTEM'")
                .foreach(row => {
                    SYSTEM.set(
                        row.getString("var_name")
                        , row.getString("var_type") match {
                            case "INTEGER" => row.getLong("var_value")
                            case "DECIMAL" => row.getDouble("var_value")
                            case _ => row.getString("var_value")
                        })

                }).clear()
    }

    //更新用户变量
    def set(name: String, value: Any, user: Int = 0): Unit = {

        var group = "NONE"
        if (USER.contains(name)) {
            group = "USER"
            USER.set(name, value)
        }
        else if (SYSTEM.contains(name)) {
            group = "SYSTEM"
            SYSTEM.set(name, value)
        }
        else if (Configurations.contains(name)) {
            Configurations.set(name, value)
        }
        else {
            throw new SQLExecuteException("Can't update system variable. This variable is read only.")
        }

        if (group != "NONE" && JDBC.hasQrossSystem) {
            val varType = value match {
                case i: Int => "INTEGER"
                case l: Long => "INTEGER"
                case f: Float => "DECIMAL"
                case d: Double => "DECIMAL"
                case _ => "STRING"
            }
            DataSource.queryUpdate(s"""INSERT INTO (var_group, var_type, var_name, var_value) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE value=?""", group, varType, name, value, value)
        }
    }

    //得到全局变量的值
    def get(name: String): DataCell = {
        val field = name.toUpperCase()
        if (USER.contains(field)) {
            USER.getCell(field)
        }
        else if (SYSTEM.contains(field)) {
            SYSTEM.getCell(field)
        }
        else if (PROGRESS.contains(field)) {
            PROGRESS.getCell(field)
        }
        else if (GLOBALS.contains(field)) {
            new DataCell(Global.getClass.getMethod(field).invoke(null))
        }
        else {
            new DataCell(null)
        }
    }
}
