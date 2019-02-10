package io.qross.util

import java.util.concurrent.ConcurrentLinkedQueue

object Mover {

    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()

}

class Mover(source: DataSource,
                 cube: Cube,
                 selectSQL: String,
                 param: String = "@offset",
                 pageSize: Int = 10000) extends Thread {

    override def run(): Unit = {
        val ds = new DataSource(source.connectionName, source.databaseName)
        while (!cube.isClosed) {
            val table = ds.executeDataTable(selectSQL.replace(param, String.valueOf(cube.sum() * pageSize)))
            //无数据也会执行，以保证会正确创建表
            Mover.DATA.add(table)

            if (table.nonEmpty) {
                while (Mover.DATA.size() >= 3) {
                    Timer.sleep(0.1F)
                }
            }
            else {
                cube.close()
            }
        }
        ds.close()

        Output.writeMessage("Thread Exit!")
    }
}
