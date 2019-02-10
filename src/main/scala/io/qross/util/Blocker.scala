package io.qross.util

import java.util.concurrent.ConcurrentLinkedQueue

object Blocker {
    val QUEUE = new ConcurrentLinkedQueue[String]()
    val DATA = new ConcurrentLinkedQueue[DataTable]()
}

class Blocker(source: DataSource) extends Thread {

    override def run(): Unit = {
        val ds = new DataSource(source.connectionName, source.databaseName)
        while (!Blocker.QUEUE.isEmpty) {
            val SQL = Blocker.QUEUE.poll()
            if (SQL != null) {
                Blocker.DATA.add(ds.executeDataTable(SQL))
            }

            while (Blocker.DATA.size() >= 3) {
                Timer.sleep(0.1F)
            }
        }
        ds.close()
    }
}
