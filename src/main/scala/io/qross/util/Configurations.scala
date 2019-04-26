package io.qross.util

import io.qross.core.DataRow
import io.qross.jdbc.{DataSource, JDBC}

object Configurations {

    private val CONFIG = DataRow()

    if (JDBC.hasQrossSystem) {
        DataSource.queryDataTable("SELECT conf_key, conf_value FROM qross_conf")
                .foreach(row => {
                    CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
                }).clear()
    }

    def get(name: String): String = {
        CONFIG.getString(name)
    }

    def set(name: String, value: Any): Unit = {
        CONFIG.set(name, value)
    }

    //如果优先从配置项中获取，如果不存在 ，则从Properties中获取
    def getOrProperty(name: String, propertyName: String, defaultValue: String = ""): String = {
        CONFIG.getStringOption(name).getOrElse(Properties.get(propertyName, defaultValue))
    }

    //优先从Properties中获取，如果不存在，则从配置项中获取
    def getPropertyOrElse(propertyName: String, configurationName: String, defaultValue: String = ""): String = {
        if (Properties.contains(propertyName)) {
            Properties.get(propertyName, defaultValue)
        }
        else {
            CONFIG.getStringOption(configurationName).getOrElse(defaultValue)
        }
    }

    //如果配置项不存在 ，则设置默认值
    def getOrElse(name: String, defaultValue: Any): Any = {
        CONFIG.get(name) match {
            case Some(value) => value
            case None => defaultValue
        }
    }
}