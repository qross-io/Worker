package io.qross.util

import io.qross.core.DataRow

object Global {
    val CORES: Int = Runtime.getRuntime.availableProcessors
    val USER_HOME_DIR: String = System.getProperty("user.dir")
    val QROSS_WORK_DIR: String = USER_HOME_DIR + "/qross/"
}
