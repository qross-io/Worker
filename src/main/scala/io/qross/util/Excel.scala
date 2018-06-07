package io.qross.util

import java.io.{File, FileInputStream, FileOutputStream}

import io.qross.core.{DataTable, DataType}
import io.qross.core.DataType.DataType
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook


object Excel {
    def main(args: Array[String]): Unit = {
        val table = DataTable()
        table.insertRow("a" -> 1, "b" -> 5, "c" -> "hello1", "d" -> "world5")
        table.insertRow("a" -> 2, "b" -> 4, "c" -> "hello2", "d" -> "world4")
        table.insertRow("a" -> 3, "b" -> 3, "c" -> "hello3", "d" -> "world3")
        table.insertRow("a" -> 4, "b" -> 2, "c" -> "hello4", "d" -> "world2")
        table.insertRow("a" -> 5, "b" -> 1, "c" -> "hello5", "d" -> "world1")
        
        Excel("d:/test.xlsx").writeTable(table, "sheet1").writeTable(table, "sheet2").save()
    }
}

case class Excel(fileName: String) {
    
    // file not exists, new file, new sheet
    // file exists, sheet not exists, new sheet only
    // file exists, sheet exists, overwrite
    
    //def saveAsExcel(fileName: String, sheetName: String = "sheet1")
    //def openExcel(fileName: String).sheet("sheetName")
    
    //Excel(fileName: String).writeTable(table: DataTable, sheetName: String = "sheet1").save()
    
    
    
    private val FASHION = "2007+"
    private val CLASSIC = "2003-"
    private val WRONG = "not a excel file"
    
    private val path = FilePath.locate(if (fileName.contains(".")) fileName else fileName + ".xlsx")
    private val version: String =
        fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase() match {
            case "xls" => CLASSIC
            case "xlsx" => FASHION
            case _ => WRONG
        }
        
    if (version == WRONG) {
        throw new Exception("Wrong file type. You must specify an excel file.")
    }
    
    private val workbook: Workbook = if (version == FASHION) new SXSSFWorkbook() else new HSSFWorkbook()
    
    def writeTable(table: DataTable, sheetName: String = "sheet1"): Excel = {
        val sheet = workbook.createSheet(sheetName) //XSSFSheet-2007, HSSFSheet-2003
        
        val head = sheet.createRow(0) // XSSFRow-2007 HSSFRow-2003
        //val cell = row.createCell(0) //XSSFCell-2007 HSSFCell-2003
        val labels = table.getLabelNames
        for (i <- labels.indices) {
            head.createCell(i).setCellValue(labels(i))
        }
        
        val rows = table.getRows
        for (i <- rows.indices) {
            val row = sheet.createRow(i + 1)
            val fields = rows(i).getFields
            val types = rows(i).getDataTypes
            for (j <- fields.indices) {
                val cell = row.createCell(j)
                types(j) match {
                    case DataType.INTEGER => cell.setCellValue(rows(i).getLong(fields(j)))
                    case DataType.DECIMAL => cell.setCellValue(rows(i).getDouble(fields(j)))
                    case _ => cell.setCellValue(rows(i).getString(fields(j)))
                }
            }
        }
        
        this
    }
    
    def save(): Excel = {
        val fos = new FileOutputStream(new File(path)) //can't not be true, or file will be wrong.
        workbook.write(fos)
        workbook.close()
        
        this
    }
    
    def attachToEmail(title: String): Email = {
        Email.write(title).attach(path)
    }
    
    def readTable(sheetName: String = "sheet1", startRow: Int = 0, startColumn: Int = 0)(fields: (String, DataType)*): DataTable = {
    
        val fis = new FileInputStream(new File(path))
        val workbook: Workbook = if (version == FASHION) new XSSFWorkbook(fis) else new HSSFWorkbook(fis)
        DataTable()
    
        /*
        //获取工作表
        val sheet = workbook.getSheet(sheetName)
        //获取行,行号作为参数传递给getRow方法,第一行从0开始计算
        val row = sheet.getRow(0)
    
        //获取单元格,row已经确定了行号,列号作为参数传递给getCell,第一列从0开始计算
        val cell = row.getCell(2)
        //设置单元格的值,即C1的值(第一行,第三列)
        val cellValue = cell.getStringCellValue
        System.out.println("第一行第三列的值是" + cellValue)
    
        //sheet.getPhysicalNumberOfRows
        //sheet.getLastRowNum
        //row.getLastCellNum
    
        workbook.close
    
        DataTable()
        */
    }
}
