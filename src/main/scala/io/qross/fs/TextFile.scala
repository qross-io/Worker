package io.qross.fs

abstract class TextFile {
    
    def openFile(fileName: String)
    def writeFile(fileName: String)
    
    def readLine(): String
    def writeLine(line: String)
    
    def close()
}
