
import io.qross.ext.TypeExt._

object Main {

    def main(args: Array[String]): Unit = {

        val SQL = """select * from abc""".stripMargin

        println(SQL.takeAfter(""))
        println(SQL.takeAfter("""\sF""".r))


    }

}
