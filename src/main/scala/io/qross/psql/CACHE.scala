package io.qross.psql

import io.qross.util.DateTime
import scala.collection.mutable

object CACHE {

    private val ALL = new mutable.HashMap[String, mutable.HashMap[String, Any]]
    private val EXPIRE = new mutable.HashMap[String, mutable.HashMap[String, Long]]

    private var lastHour = DateTime.now.getHour

    def set(apiName: String, value: Any): Unit = {
        set(apiName, "", value)
    }

    def set(apiName: String, params: String, value: Any): Unit = {
        if (!ALL.contains(apiName)) {
            ALL.put(apiName, new mutable.HashMap[String, Any])
            EXPIRE.put(apiName, new mutable.HashMap[String, Long])
        }
        ALL(apiName).put(params, value)
        EXPIRE(apiName).put(params, DateTime.now.toEpochSecond)
    }


    def get(apiName: String, params: String = ""): Any = {
        if (ALL.contains(apiName) && ALL.get(apiName).contains(params)) {
            if (!EXPIRE.contains(apiName)) {
                EXPIRE.put(apiName, new mutable.HashMap[String, Long])
            }
            EXPIRE(apiName).put(params, DateTime.now.toEpochSecond)
            ALL(apiName)(params)
        }
        else {
            null
        }
    }

    def contains(apiName: String, params: String = ""): Boolean = {
        CACHE.clean()
        ALL.contains(apiName) && ALL(apiName).contains(params)
    }

    def remove(apiName: String): Unit = {
        ALL.remove(apiName)
    }

    def remove(apiName: String, params: String): Unit = {
        if (ALL.contains(apiName) && ALL(apiName).contains(params)) {
            ALL(apiName).remove(params)
        }
    }

    def clean(): Unit = {
        val now = DateTime.now
        val hour = now.getHour
        if (hour != lastHour) {
            val second = now.toEpochSecond
            //待清除列表
            val nps = new mutable.ArrayBuffer[NameAndParams]
            for (name <- EXPIRE.keySet) {
                for (params <- EXPIRE(name).keySet) {
                    if (second - EXPIRE(name)(params) >= 3600) {
                        nps += new NameAndParams(name, params)
                    }
                }
            }

            for (np <- nps) {
                EXPIRE(np.name).remove(np.params)
                ALL(np.name).remove(np.params)
            }

            nps.clear()
            lastHour = hour
        }
    }
}


case class NameAndParams(var name: String, var params: String)