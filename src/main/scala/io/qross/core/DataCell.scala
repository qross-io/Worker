package io.qross.core

import io.qross.jdbc.DataType
import io.qross.jdbc.DataType.DataType

class DataCell(val value: Any, var dataType: DataType = DataType.NULL) {
    if (value != null && dataType == DataType.NULL) {
        dataType = DataType.of(value)
    }

    override def toString: String = {
        if (value == null) {
            null
        }
        else {
            value.toString
        }
    }
}
