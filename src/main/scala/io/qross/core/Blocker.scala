package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

object Blocker {
    val CUBE = new Cube()
    val QUEUE = new ConcurrentLinkedQueue[String]()
    val DATA = new ConcurrentLinkedQueue[DataTable]()
}

class Blocker(source: DataSource, tanks: Int = 3) extends Thread {

    override def run(): Unit = {

        Blocker.CUBE.increase()

        val ds = new DataSource(source.connectionName, source.databaseName)

        while (!Blocker.QUEUE.isEmpty) {
            val SQL = Blocker.QUEUE.poll()
            if (SQL != null) {
                Blocker.DATA.add(ds.executeDataTable(SQL))
            }

            while (Blocker.DATA.size() >= tanks) {
                Timer.sleep(0.1F)
            }
        }

        ds.close()
        Blocker.CUBE.close()

        Output.writeMessage("Blocker Thread Exit!")
    }
}
