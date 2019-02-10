package io.qross.util

class FileReaderConsumer(cube: Cube, handler: DataTable => DataTable) extends Thread {

    override def run(): Unit = {
        while (!cube.isClosed || !FileReader.DATA.isEmpty) {
            val table = FileReader.DATA.poll()
            if (table != null) {
                handler(table)
            }
            table.clear()

            Timer.sleep(0.1F)
        }
    }
}
