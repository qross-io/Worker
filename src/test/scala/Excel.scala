import io.qross.core.DataHub
import io.qross.fs.Excel._

object Excel {
    def main(args: Array[String]): Unit = {

        val dh = new DataHub()

        dh.saveAsNewExcel("d:/test.xlsx")
                .writeCellValue("2018年09月05日催收公司信息", "催收公司详情")
                .mergeCells(0 -> 0, 0 -> 4)
                .withCellStyle(BackgroundColor -> Color.Green, FontSize -> 16, FontStyle -> Font.Bold, BorderStyle -> Border.Dashed, Align -> Center, ColumnWidth -> Auto)

                .insertRow("统计日期" -> "2018-09-05",
                    "当日注册数" -> 5,
                    "当日新增数" -> 2,
                    "当日在库数" -> 1634,
                    "当日活跃数" -> 306)
                .writeSheetWithHeader("催收公司详情", 1)
                .withHeaderStyle(FontStyle -> Font.Bold, BorderStyle -> Border.Thin, BackgroundColor -> Color.Grey25Percent, Align -> Center, ColumnWidth -> Auto)
                .withRowStyle(BorderStyle -> Border.Thin, Align -> Center)

        dh.close()
    }
}
