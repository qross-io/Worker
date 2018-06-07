package io.qross.util

import java.io.File

object FilePath {
    
    def locate(path: String): String = {
        var full = path.replace("\\", "/")
        if (!full.startsWith("/") && !full.contains(":/")) {
            full = Global.DATA_HUB_DIR + full
        }
        val parent = new File(full).getParentFile
        if (!parent.exists()) {
            parent.mkdir()
        }
    
        full
    }
    
}
