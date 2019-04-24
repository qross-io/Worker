package io.qross.fs

import io.qross.core.DataRow
import io.qross.net.Email

import scala.io.Source

case class OpenResourceFile(path: String) {

    private val source = Source.fromInputStream(this.getClass.getResourceAsStream(path), "UTF-8")
    private lazy val content: String = source.mkString
    private lazy val lines: Iterator[String] = source.getLines()
    private var output: String = ""
    
    def foreach(callback: (String) => Unit): OpenResourceFile = {
        lines.foreach(line => callback(line))
        this
    }
    
    def replace(oldStr: String, newStr: String): OpenResourceFile = {
        if (output == "") output = content
        output = output.replace(oldStr, newStr)
        this
    }
    
    def replaceWith(row: DataRow): OpenResourceFile = {
        if (output == "") output = content
        row.foreach((key, value) => {
            output = output.replace("${" + key + "}", if (value != null) value.toString else "")
        })
        this
    }
    
    def writeEmail(title: String): Email = {
        source.close()
        if (output == "") output = content
        Email.write(title).setContent(output)
    }
    
    def close(): Unit = {
        source.close()
    }

    def getContent(): String = {
        if (output == "") output = content
        source.close()
        output
    }
}