package io.qross.worker

import java.io.{File, FileInputStream}

import io.qross.core.{DataTable, DataType}
import io.qross.jdbc.DataSource
import io.qross.util._
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.collection.immutable.HashSet

object Test {
    
    def main(args: Array[String]): Unit = {
        insertTasks()
    }
    
    def insertTasks(): Unit = {
        val ds = new DataSource("mysql.bi_platform")
//        DateTime.now.sharp("FROM MONTH=4#DAY=1#HOUR=1#MINUTE=10#SECOND=0 TO MONTH=5#DAY=2#HOUR=23#MINUT=10#SECOND=0 FILTER MINUTE=10#SECOND=0 FORMAT yyyyMMddHHmmss").foreach(time => {
//            ds.executeNonQuery(s"INSERT INTO qross_tasks (job_id, task_time) VALUES (110, '$time');")
//        })
        ds.close()
    }
    
    def joinMallyUsers(): Unit = {
        var activity = new HashSet[String]()
        var full = new HashSet[String]()
    
        val reader1 = FileReader("d:\\20180508.txt")
        while(reader1.hasNextLine) {
            val line = reader1.readLine
            if (line.contains("\t")) {
                full += line.substring(line.indexOf("\t") + 1).trim()
            }
        }
        reader1.close()
        
        println(full.size)
    
        val reader2 = FileReader("d:\\20180508.csv")
        while(reader2.hasNextLine) {
            activity += reader2.readLine.trim()
        }
        reader2.close()
        
        println(activity.size)
    
        val table = DataTable()
        table.addField("gid", DataType.TEXT)
        activity.intersect(full).foreach(row => {
            table.insertRow("gid" -> row)
        })
    
        Excel("D:\\20180508i.xlsx").writeTable(table)
        
        table.clear()
        /*
        println(1)
        val fis = new FileInputStream(new File("d:\\20180508.xlsx"))
        println(2)
        val workbook: Workbook = new XSSFWorkbook(fis)
        println(3)
        
        //获取工作表
        val sheet = workbook.getSheet("sheet1")
        println(4)
        
        var i = 1
        var break = false
        while (!break) {
            val row = sheet.getRow(i)
            if (row != null) {
                val cell = row.getCell(0)
                activity += cell.getStringCellValue
                i += 1
            }
            else {
                break = true
            }
            
            if (i > 10) {
                break = true
            }
            println(i)
        }
        println(5)
        workbook.close()
        fis.close()
        */
    }
    
    def insertTask(): Unit = {
        
        val beginTime = DateTime.of(2018, 5, 3, 17, 10, 0)
        val endTime = DateTime.of(2018, 5, 3, 0, 10, 0)
        val ds = new DataSource("mysql.bi_platform")
        while (beginTime.afterOrEquals(endTime)) {
            println(beginTime)
            ds.executeNonQuery(s"INSERT INTO qross_tasks (job_id, task_time) VALUES (110, ${beginTime.getString("yyyyMMddHHmmss")})")
            beginTime.minusHours(1)
        }
        ds.close()
        
    }
    
    def sliceFile(): Unit = {
        
        val header = "ugid,uid,大额额度,普通额度,实名时间,是否手机认证,是否全部还完,最长逾期时间（秒）,受否有在借,最早一次访问时间（3月2日起）"
        val reader = FileReader("e:\\qiuyue_20180502_wechat_2.csv")
        var f = 0
        var i = 0
        var writer = FileWriter(s"e:\\qiuyue_20180502_wechat_2_$f.csv")
        writer.writeLine(header)
        while (reader.hasNextLine) {
            writer.writeLine(reader.readLine)
            
            i += 1
            if (i % 500000 == 0) {
                writer.close()
                println(i)
                f += 1
                writer = FileWriter(s"e:\\qiuyue_20180502_wechat_2_$f.csv")
                writer.writeLine(header)
            }
        }
    
        if (i % 500000 > 0) {
            writer.close()
        }
        
        println(i)
        reader.close()
    }
}
