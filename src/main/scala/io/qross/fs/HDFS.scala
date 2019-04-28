package io.qross.ext

import java.io.IOException

import io.qross.setting.Global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object HDFS {

    private val conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    
    if (Global.KERBEROS_AUTH) {
        KerberosLogin.login(Global.KRB_USER_PRINCIPAL, Global.KRB_KEYTAB_PATH, Global.KRB_KRB5CONF_PATH, conf)
    }
    
    private lazy val fileSystem = FileSystem.get(conf)
    
    def list(path: String): List[HDFS] = {
        val dir = new Path(path)
        val list = new mutable.ListBuffer[HDFS]()
        
        try {
            val files = fileSystem.globStatus(dir)
            if (files.nonEmpty) {
                for (f <- files) {
                    var p = f.getPath.toString
                    p = p.substring(p.indexOf("//") + 2)
                    p = p.substring(p.indexOf("/"))
                
                    list += HDFS(p, f.getLen, f.getModificationTime)
                    //f.getLen - byte
                    //f.getModificationTime - ms
                }
            }
        } catch {
            case e: IOException => e.printStackTrace()
        }
    
        list.toList
    }
}

case class HDFS(path: String, size: Long, lastModificationTime: Long)