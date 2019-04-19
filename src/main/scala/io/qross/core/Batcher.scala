package io.qross.core

class Batcher(source: DataSource, sentence: String) extends Thread {

    override def run(): Unit = {

        val ds = new DataSource(source.connectionName, source.databaseName)
        val index = Processer.DATA.size - 1
        while (
            if (index == -1)
                !Pager.CUBE.isClosed || !Pager.DATA.isEmpty || !Blocker.CUBE.isClosed || !Blocker.DATA.isEmpty
            else
                !Processer.isClosed
        ) {

            val table: DataTable =
                if (index == -1) {
                    if (!Pager.DATA.isEmpty) {
                        Pager.DATA.poll()
                    }
                    else if (!Blocker.DATA.isEmpty) {
                        Blocker.DATA.poll()
                    }
                    else {
                        null
                    }
                }
                else {
                    Processer.DATA(index).poll()
                }

            if (table != null) {
                ds.tableUpdate(sentence, table)
            }

            Timer.sleep(0.1F)
        }

        ds.close()

        Output.writeMessage("Batcher Thread Exit!")
    }
}
