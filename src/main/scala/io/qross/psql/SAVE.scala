package io.qross.psql

import io.qross.setting.Properties
import io.qross.ext.TypeExt._

/*
SAVE AS connectionName;
SAVE AS connectionName USE databaseName;
SAVE AS DEFAULT;
SAVE AS CACHE;
SAVE AS CACHE TABLE tableName;
SAVE AS TEMP;
SAVE AS TEMP TABLE tableName;
*/

class SAVE(var targetType: String, var target: String, var use: String = "") {
    if (targetType != null) {
        targetType = targetType.$replace("""\s\s""", " ").toUpperCase()
    }

    targetType match {
        case "CACHE TABLE" =>
        case "TEMP TABLE" =>
        case null =>
        case _ =>
            if (target.matches("(?i)^(CACHE|TEMP|DEFAULT)$")) {
                target = target.toUpperCase()
            }
            else {
                if (!Properties.contains(target)) {
                    throw new SQLParserException("Wrong connection name: " + target)
                }
            }

    }

    if (use == null) {
        use = ""
    }
}
