package io.qross.util

import java.io.{File, FileInputStream, IOException}
import java.util.Scanner
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

case class FileReader(filePath: String) {
    
    private val CHARSET = "utf-8"
    
    private var file = new File(FilePath.locate(filePath))
    if (!file.exists) throw new IOException("File not found: " + filePath)
    private val extension = filePath.substring(filePath.lastIndexOf("."))
    
    private var scanner: Scanner =
        if (".log".equalsIgnoreCase(extension) || ".txt".equalsIgnoreCase(extension) || ".csv".equalsIgnoreCase(extension)) {
            new Scanner(this.file, CHARSET)
        }
        else if (".gz".equalsIgnoreCase(extension)) {
            new Scanner(new GZIPInputStream(new FileInputStream(this.file)), CHARSET)
        }
        else if (".bz2".equalsIgnoreCase(extension)) {
            new Scanner(new BZip2CompressorInputStream(new FileInputStream(this.file)), CHARSET)
        }
        else {
            throw new IOException("Unrecognized Format: " + this.filePath)
        }
        
    def hasNextLine: Boolean = scanner.hasNextLine
        
    def readLine: String = scanner.nextLine
        
    def close(): Unit = {
        scanner.close()
    }
}
    

