
import java.util.regex.Pattern

import io.qross.ext.TypeExt._
//import io.qross.psql.Patterns._

object Main {

    def main(args: Array[String]): Unit = {

        //val SQL = """select * from abc""".stripMargin
        //println(SQL.takeAfter(""))
        //println(SQL.takeAfter("""\sF""".r))

        println((1f/2).floor(2))

        val $OPEN: Pattern = Pattern.compile("""^OPEN\s+((\S+\s+)*?)(\S+)(\s+USE\s+(\S+))?\s*(:|$)""", Pattern.CASE_INSENSITIVE)
        val m = $OPEN.matcher("""OPEN  mysql.rds USE qross""")
        if (m.find()) {
            println(m.group())
            println(m.group(1))
            println(m.group(2))
            println(m.group(3))
            println(m.group(4))
            println(m.group(5))
        }
        else {
            println("NO")
        }

    }

}
