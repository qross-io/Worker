import java.util
import java.util.regex.Pattern

import io.qross.core.DataHub
import io.qross.ext.PlaceHolder
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataSource
import io.qross.psql.PSQL._
import io.qross.psql.Patterns
import io.qross.psql.Patterns._
import io.qross.setting.Global
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto.Database

import scala.collection.mutable

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

//        println(Global.getClass)
//        println(Global.getClass.getDeclaredMethod("CORES"))
//        println(Class.forName("io.qross.setting.Global").getDeclaredMethod("CORES").invoke(null))
//        Global.getClass.getDeclaredMethod("CORES").invoke(null)


        //System.exit(0)

        val dh = new DataHub()

        dh.openDefault()

         //.signIn(1, "Garfi", "MASTER")
        dh.signIn(5, "wuzheng")

//        dh.openDefault()
//            .get("SELECT * FROM qross_variables")
//                .cache("ars")
//        .openCache()
//            .get("SELECT var_name, var_value FROM ars")
//        .saveAsDefault()
//            .put("INSERT INTO tc (status, info) VALUES ('#var_name', &var_value)")

        dh.run(
            """
                PRINT "HELLO WORLD";
                OPEN DEFAULT:
                    CACHE "ars" # SELECT * FROM qross_variables;
                OPEN CACHE:
                    GET # SELECT var_name, var_value FROM ars;
                LIST 20;
                SAVE AS DEFAULT:
                    PUT # INSERT INTO tc (status, info) VALUES ('#var_name', &var_value);
            """)

        dh.close()
    }
}

