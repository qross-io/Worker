package io.qross.util

import java.io._

import scala.util.{Success, Try}

object Properties {
   
    private val props = new java.util.Properties()

    //private val internalPath = Properties.getClass.getResource("/conf.properties").toString.replace("jar:", "").replace("file:/", "")
    //private val internalStream = Properties.getClass.getResourceAsStream("/conf.properties")
    //private lazy val externalOutput = new FileOutputStream(internalPath)
    
    //props.load(new BufferedInputStream(new FileInputStream(new File(externalPath))))

    private val onlineFile =  new File("/azkaban/datahub/conf.properties")
    if (onlineFile.exists()) {
        props.load(new BufferedInputStream(new FileInputStream(onlineFile)))
    }
    else {
        //local
        val externalFile = new File(new File(Properties.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath.replace("\\", "/") + "/dbs.properties")
        if (externalFile.exists()) {
            props.load(new BufferedInputStream(new FileInputStream(externalFile)))
        }
        //in jar
        else {
            props.load(new BufferedReader(new InputStreamReader(Properties.getClass.getResourceAsStream("/conf.properties"))))
        }
    }

    def contains(key: String): Boolean = {
        props.containsKey(key)
    }
    
    def get(key: String, defaultValue: String = ""): String = {
        if (props.containsKey(key)) {
            props.getProperty(key)
        }
        else {
            defaultValue
        }
    }
    
    def getInt(key: String, defaultValue: Int = 0): Int = {
        if (props.containsKey(key)) {
            Try(props.getProperty(key).toInt) match {
                case Success(value) => value
                case _ => defaultValue
            }
        }
        else {
            defaultValue
        }
    }
    
    def getBoolean(key: String): Boolean = {
        if (props.containsKey(key)) {
            props.getProperty(key).toLowerCase() match {
                case "true" | "yes" | "ok" | "1" => true
                case _ => false
            }
        }
        else {
            false
        }
    }
    
    /*
    def set(key: String, value: String): Unit = {
        props.setProperty(key, value)
        props.store(externalOutput, "updated by user: " + key + " = " + value)
    }
    
    def getDataSources: HashMap[String, String] = {
        var sources = new HashMap[String, String]
        props.entrySet().forEach(row => {
            sources += (row.getKey.toString -> row.getValue.toString)
        })
        
        sources
    }*/
}