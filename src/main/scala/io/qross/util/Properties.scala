package io.qross.util

import java.io._

import io.qross.jdbc.{JDBConnection, MySQL}
import org.sqlite.JDBC

import scala.util.{Success, Try}

object Properties {
    
    private val props = new java.util.Properties()
    private val externalPath = new File(Properties.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath.replace("\\", "/") + "/qross.ds.properties"
    //private val internalPath = Properties.getClass.getResource("/conf.properties").toString.replace("jar:", "").replace("file:/", "")
    private val internalStream = Properties.getClass.getResourceAsStream("/conf.properties")
    //private lazy val externalOutput = new FileOutputStream(internalPath)
    
    props.load(new BufferedInputStream(new FileInputStream(new File(externalPath))))
    props.load(new BufferedReader(new InputStreamReader(internalStream)))
    
    private val extraPath = Properties.get("extra_properties")
    if (extraPath != "") props.load(new BufferedInputStream(new FileInputStream(new File(externalPath))))
    
    if (!props.containsKey(JDBConnection.PRIMARY)) {
        Output.writeExceptions("Can't find properties key mysql.qross, it must be set in conf.properties in resources directory.")
        System.exit(1)
    }
    else if (!MySQL.testConnection()) {
        Output.writeExceptions("Can't open primary database, please check your connection string of mysql.qross in conf.properties.")
        System.exit(1)
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
    
    def contains(key: String): Boolean = {
        props.containsKey(key)
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