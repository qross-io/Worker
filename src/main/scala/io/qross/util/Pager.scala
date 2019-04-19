package io.qross.util

import java.util.concurrent.ConcurrentLinkedQueue

object Pager {
    val CUBE = new Cube()
    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()
}

class Pager(source: DataSource,
            selectSQL: String,
            param: String = "@offset",
            pageSize: Int = 10000, tanks: Int = 3) extends Thread {

    override def run(): Unit = {

        Pager.CUBE.mark()

        val ds = new DataSource(source.connectionName, source.databaseName)
        var break = false
        do {
            while (Pager.DATA.size() >= tanks) {
                Timer.sleep(0.5F)
            }

            val table = ds.executeDataTable(selectSQL.replace(param, String.valueOf(Pager.CUBE.increase() * pageSize)))

            //无数据也得执行，以保证会正确创建表
            Pager.DATA.add(table)

            if (table.isEmpty) {
                Pager.CUBE.wipe()
                break = true
            }

        } while (!break)

        ds.close()

        Output.writeMessage("Pager Thread Exit!")
    }
}
