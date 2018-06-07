package io.qross.util

import io.qross.core.DataRow

object Global {
    val CORES: Int = Runtime.getRuntime.availableProcessors
    val HOME_PATH: String = System.getProperty("user.dir")
    val DATA_HUB_DIR: String = HOME_PATH + "/datahub/"
}
