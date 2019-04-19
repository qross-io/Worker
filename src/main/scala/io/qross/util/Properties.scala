package io.qross.util

import java.io._

import io.qross.util.PropertiesK.props

import scala.collection.mutable.ArrayBuffer

object Properties {

    /*
    加载顺序
    与jar包同一目录下的 qross.properties
    与jar包同一目录下的 dbs.properties
    jar包运行参数 --properties 后的所有 properties文件 适用于worker
            jar包内的conf.properties
    数据库中的 properties / qross_properties
            数据库中的 连接串
            连接名冲突时先到先得
    将所有连接串保存在 JDBC.connections中
    */

    private val props = new java.util.Properties()
    private val externalPath = new File(PropertiesK.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath.replace("\\", "/")

    loadLocalFile(externalPath + "/qross.properties")
    loadLocalFile(externalPath + "/dbs.properties")
    loadResourcesFile("/conf.properties")

    def loadLocalFile(path: String): Boolean = {
        val file = new File(path)
        if (file.exists()) {
            props.load(new BufferedInputStream(new FileInputStream(file)))
            true
        }
        else {
            false
        }
    }

    def loadResourcesFile(path: String): Boolean = {
        try {
            props.load(new BufferedReader(new InputStreamReader(PropertiesK.getClass.getResourceAsStream(path))))
            true
        }
        catch {
            case _ : Exception => false
        }
    }

}
