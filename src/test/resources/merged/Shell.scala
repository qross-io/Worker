package io.qross.util

import scala.sys.process._

object Shell {
    
    def run(commandText: String): Int = {
        val exitValue = commandText.!(ProcessLogger(out => {
            println(out)
        }, err => {
            //System.err.println(err)
            println(err)
        }))
        
        exitValue
    }
    
    def go(commandText: String): Int = {
        
        val process = commandText.run(ProcessLogger(out => {
            println(out)
        }, err => {
            System.err.println(err)
        }))
    
        var i = 0
        while(process.isAlive()) {
            println("s#" + i)
            i += 1
//            if (i > 10) {
//                process.destroy()
//            }
            Timer.sleep(1)
        }
        
        println("exitValue: " + process.exitValue())
        //process.destroy()
        
        0
    }
    
    def main(args: Array[String]): Unit = {
        go("java -cp C:/weshare/test/build/libs/test-1.0.jar io.qross.test.Main")
    }
}
