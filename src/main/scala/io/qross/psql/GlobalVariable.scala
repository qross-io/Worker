package io.qross.psql

import io.qross.core.DataRow
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.{Global, Properties}
import io.qross.time.DateTime

object GlobalVariable {

    val INSTANTS: Set[String] = Set("NOW", "TODAY")
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
    def get(name: String): Any = {
        val field = name.toUpperCase()
        if (USER.contains(field)) {
            USER.get(field).orNull
        }
        else if (SYSTEM.contains(field)) {
            SYSTEM.get(field).orNull
        }
        else if (INSTANTS.contains(field)) {
            field match {
                case "NOW" => DateTime.now
                case "TODAY" => DateTime.now.setZeroOfDay()
            }
        }
        else if (PROGRESS.contains(field)) {
            PROGRESS.get(field).orNull
        }
        else if (GLOBALS.contains(field)) {
            Global.getClass.getMethod(field).invoke(null)
        }
        else {
            null
        }
    }
}
