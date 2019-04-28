package io.qross.fs

import java.io.File

import io.qross.setting.Global
import io.qross.ext.TypeExt._

object FilePath {
    
    def locate(path: String): String = {
        var full = path.toPath
        
        if (!full.startsWith("/") && !full.contains(":/")) {
            if (Global.QROSS_SYSTEM == "WORKER") {
                full = Global.QROSS_WORKER_HOME  + full
            }
            else {
                full = Global.QROSS_KEEPER_HOME  + full
            }
        }
        
        val parent = new File(full).getParentFile
        if (!parent.exists()) {
            parent.mkdir()
        }

        full
    }

    def delete(path: String): Boolean = {
        val file = new File(FilePath.locate(path))
        if (file.exists() && file.isFile) {
            file.delete()
        }
        else {
            false
        }
    }
}
