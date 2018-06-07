package io.qross

import java.io.{File, FileInputStream}

import io.qross.core.DataHub
import io.qross.util._
import org.apache.poi.ss.usermodel.{Row, Workbook}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

object Main {
    def main(args: Array[String]): Unit = {
        
        //val dh = new DataHub()
        
        //        dh.open("mysql.storm").saveAs("mysql.bi_platform")
        //            .get("SELECT T2.user_gid, T3.id AS uid FROM (SELECT DISTINCT user_gid FROM (SELECT DISTINCT deposit_user_gid AS user_gid FROM record_deposit_appoint WHERE deposit_status=1 OR deposit_status=2 UNION SELECT DISTINCT user_gid FROM user_lightning WHERE first_try_time IS NOT NULL) T1) T2 INNER JOIN user_lightning T3 ON T2.user_gid=T3.user_gid")
        //            .put("INSERT INTO tmp_60004 (user_gid, uid) VALUES (?, ?)")
        
        /*
        var br = false
        var i = 1
        var id = 0
        while (!br) {
            
            println(i)
            
            val gids = dh.open("mysql.bi_platform").executeDataTable(s"SELECT user_gid FROM tmp_60004 LIMIT ${i*2000}, 1000").mkString("('", "','", "user_gid", "')")
            if (gids != "('')") {
//                dh.open("mysql.storm").saveAs("mysql.bi_platform")
//                    .get(s"SELECT CAST(is_followed AS INT) AS is_followed, user_gid FROM user_channel_wechat WHERE user_gid IN $gids")
//                        .put("UPDATE tmp_60004 SET is_followed_now=$is_followed WHERE user_gid=$user_gid")
                
                dh.open("mysql.storm").saveAs("mysql.bi_platform")
                    .get(s"SELECT user_gid, COUNT(0) AS amount FROM voucher WHERE is_valid=1 AND is_used=0 AND expire_time>unix_timestamp() AND user_gid IN $gids GROUP BY user_gid")
                        .put("UPDATE tmp_60004 SET vouchers=#amount WHERE user_gid='#user_gid'")
            }
            else {
                br = true
            }
            i += 1
        } */

        //val dh = new DataHub()
//        dh.open("mysql.storm").saveAs("mysql.local")
//        Output.writeLine("OPEN DONE!")
//        dh.get("SELECT user_gid, COUNT(0) AS amount FROM voucher WHERE is_valid=1 AND is_used=0 AND expire_time>unix_timestamp() GROUP BY user_gid")
//        Output.writeLine("GET DONE!")
//        dh.put("UPDATE tmp_8272 SET vouchers=#amount WHERE user_gid='#user_gid'")
//        Output.writeLine("PUT DONE!")
        

//        dh.open("mysql.local")
//          .get("SELECT user_gid FROM tmp_8272")
//            .flat(table => {
//                val gids = table.mkString("('", "','", "user_gid", "')")
//                DataRow("gids" -> gids)
//            })
//        .open("mysql.storm")
//            .pass("SELECT user_gid, is_followed FROM user_channel_wechat WHERE user_gid IN #gids")
//        .saveAs("mysql.local")
//            .put("UPDATE tmp_8272 SET is_followed_now=#is_followed WHERE user_gid='#user_gid'")
//
//        dh.close()
        
        /*
        dh.open("mysql.bi_platform")
                .get("SELECT user_gid FROM tmp_60004")
            .flat(table => {
                val gids = table.mkString("('", "','", "user_gid", "')")
                DataRow("gids" -> gids)
            })
            .open("mysql.storm")
                .pass("SELECT user_gid, COUNT(0) AS amount FROM voucher WHERE is_valid=1 AND is_used=0 AND expire_time>unix_timestamp() AND user_gid IN #gids GROUP BY user_gid")
            .saveAs("mysql.bi_platform")
                .put("UPDATE tmp_60004 SET vouchers=#amount WHERE user_gid='#user_gid'")
    */
        
        //        dh.open("mysql.bi_platform")
        //            .get("SELECT * FROM tmp_1079")
        //            .saveAsExcel("d:\\1079.xlsx")
        
        val dh = new DataHub()
        dh.open("mysql.caribeintl_id")
            .get("select mobile, CAST(from_unixtime(reg_time/1000) AS CHAR) as reg_time from t_user where reg_time>=unix_timestamp('2018-04-02') * 1000 AND reg_time<=unix_timestamp('2018-05-03') * 1000")
            .label("mobile" -> "手机号", "reg_time" -> "注册时间")
            .saveAsExcel("d:\\4344.xlsx")
            
        dh.close()
    
        //
        
        /*
        val date = DateTime.now.plusDays(-1)
    
        dh.open("mysql.bi_platform")
            .get("SELECT user_gid FROM tmp_1079")
            .flat(table => {
                val gids = table.mkString("('", "','", "user_gid", "')")
                DataRow("gids" -> gids)
            })
        dh.open("hive.default")
            .get(s"select distinct ugid, dm['ch_sub'], '${date.getString("yyyy-MM-dd")}' as ch_sub from t_sea_acq03 where ds='${date.getString("yyyyMMdd")}' AND substr(dm['ele_id'], 1, 3)='lc_' AND dm['ch_sub'] is not null AND ugid IN #gids")
        */
        
        /*
            .pass("SELECT user_gid, is_followed FROM user_channel_wechat WHERE user_gid IN #gids")
        .saveAs("mysql.bi_platform")
            .put("UPDATE tmp_1079 SET is_followed_now=#is_followed WHERE user_gid='#user_gid'")
            */
        
        //dh.close()
        
        /*
        val fis = new FileInputStream(new File("d:\\8272_0.xlsx"))
        val workbook: Workbook = new XSSFWorkbook(fis)
        
        //获取工作表
        val sheet = workbook.getSheet("sheet1")
        
        //Output.writeMessage(sheet.getFirstRowNum)
        //Output.writeLine(sheet.getLastRowNum)
        
        //Output.writeLine(sheet.getPhysicalNumberOfRows)
        val table = new DataTable()
        table.addField("user_gid", DataType.TEXT)
        table.addField("uid", DataType.INTEGER)
        table.addField("register_time", DataType.TEXT)
        table.addField("channel_type", DataType.TEXT)
        table.addField("loan_amount", DataType.DECIMAL)
        
        var i = 1
        var break = false
        while (!break) {
            val row = sheet.getRow(i)
            if (row != null) {
                val cell = row.getCell(0)
                table.insertRow(
                    "user_gid" -> row.getCell(0).getStringCellValue,
                    "uid" -> row.getCell(1).getNumericCellValue,
                    "register_time" -> row.getCell(2).getStringCellValue,
                    "channel_type" -> row.getCell(3).getStringCellValue,
                    "loan_amount" -> row.getCell(4).getNumericCellValue)
                
                i += 1
            }
            else {
                break = true
            }
        }
        
        //设置单元格的值,即C1的值(第一行,第三列)
        //val cellValue = cell.getStringCellValue
        //System.out.println("第一行第三列的值是" + cellValue)
        
        //sheet.getPhysicalNumberOfRows
        //sheet.getLastRowNum
        //row.getLastCellNum
        
        workbook.close()
        fis.close()
        
        
        val dh = new DataHub()
        dh.buffer(table).saveAs("mysql.local").put("INSERT INTO tmp_8272 (user_gid, uid, register_time, channel_type, loan_amount) VALUES ($user_gid, $uid, $register_time, $channel_type, $loan_amount)")
        dh.close()
        
        table.clear()
        */
    }
}
