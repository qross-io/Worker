package io.qross.util

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

//生产者-加工者-消费者模式中的加工者，可以有多个

object Processer {
    val CUBEs = new ArrayBuffer[Cube]()
    //存储生产者消费者处理模式中的中间结果（渠道）
    val DATA = new ArrayBuffer[ConcurrentLinkedQueue[DataTable]]()

    def isClosed: Boolean = {
        var closed = true
        for (cube <- CUBEs) {
            if (!cube.isClosed) {
                closed = false
            }
        }

        closed
    }
}

class Processer(source: DataSource, sentence: String, index: Int, tanks: Int = 3) extends Thread {

    Processer.CUBEs += new Cube()
    if (Processer.DATA.size == index) {
        Processer.DATA += new ConcurrentLinkedQueue[DataTable]()
    }

    override def run(): Unit = {

        Processer.CUBEs(index).increase()
        Processer.CUBEs(index).mark()

        val ds = new DataSource(source.connectionName, source.databaseName)

        while (
            if (index == 0)
                !Pager.CUBE.isClosed || !Pager.DATA.isEmpty || !Blocker.CUBE.isClosed || !Blocker.DATA.isEmpty
            else
                !Processer.CUBEs(index-1).isClosed || !Processer.DATA(index - 1).isEmpty
        ) {
            val table: DataTable = if (index == 0) {
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
                Processer.DATA(index - 1).poll()
            }

            if (table != null) {
                Processer.DATA(index).add(ds.tableSelect(sentence, table))
            }

            do {
                Timer.sleep(0.1F)
            }
            while (Processer.DATA(index).size() >= tanks)
        }

        ds.close()

        Processer.CUBEs(index).wipe()

        Output.writeMessage("Processer Thread Exit!")
    }
}
