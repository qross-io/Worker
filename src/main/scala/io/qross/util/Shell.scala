package io.qross.util

import scala.sys.process._

object Shell {
    
    def run(commandText: String): Int = {
        val exitValue = commandText.!(ProcessLogger(out => {
            println(out)
        }, err => {
            System.err.println(err)
        }))
        
        exitValue
    }
}
