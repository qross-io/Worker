package io.qross.psql

import io.qross.ext.TypeExt._
import io.qross.setting.Properties

/*
OPEN connectionName;
OPEN connectionName USE databaseName;
OPEN CACHE;
OPEN TEMP;
OPEN DEFAULT;
*/

class OPEN(var sourceType: String, var source: String, var use: String = "") {
    sourceType = sourceType.$replace("""\s\s""", " ")
    if (use == null) {
        use = ""
    }

    if (source.matches("(?i)^(CACHE|TEMP|DEFAULT)$")) {
        source = source.toUpperCase()
    }
    else {
        if (!Properties.contains(source)) {
            throw new SQLParseException("Wrong connection name: " + source)
        }
    }
}
