import java.util.regex.Pattern

import io.qross.core.DataHub
import io.qross.ext.PlaceHolder
import io.qross.ext.TypeExt._
import io.qross.psql.PSQL._
import io.qross.psql.Patterns
import io.qross.psql.Patterns._

object PSQLTest {

    def main(args: Array[String]): Unit = {

//        """(^|[^\$])(\$\(?([a-zA-Z0-9_]+)\)?)""".r.findAllMatchIn("$s + ' 123'").foreach(m => {
//            println(m.group(0))
//            println(m.group(1))
//            println(m.group(2))
//            println(m.group(3))
//        })


//        val m = Pattern.compile("""^PRINT\s+?([a-z]+\s+)?(.+)$""", Pattern.CASE_INSENSITIVE).matcher("""PRINT "hello world!"""")
//        m.find()
//        println(m.group(0))
//        println(m.group(1))
//        println(m.group(2))

//        println("123".takeAfter(0))
//
//        """^SET\s+([@\$].+?):=(.+)$""".r.findAllMatchIn("""SET $s, $t := 1,2""").foreach(m => {
//            println(m.group(0))
//            println(m.group(1))
//            println(m.group(2).trim)
//        })
//

//        System.exit(0)

        val dh = new DataHub()

        dh.signIn(1, "Garfi", "MASTER")
                .run(
            """SET @s := 6;
              | PRINT @today""".stripMargin)

        dh.close()
    }
}

