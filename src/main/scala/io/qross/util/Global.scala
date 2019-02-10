package io.qross.util

object Global {

    val CORES: Int = Runtime.getRuntime.availableProcessors
    val HOME_PATH: String = System.getProperty("user.dir").replace("\\", "/")
    val DATA_HUB_DIR: String = HOME_PATH + "/datahub/"

    val EXCEL_TEMPLATES_PATH: String = DataSource.querySingleValue("SELECT conf_value FROM qross_conf WHERE conf_key='EXCEL_TEMPLATES_PATH'").getOrElse("")
}
