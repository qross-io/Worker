package io.qross.core

import io.qross.jdbc.DataType
import io.qross.jdbc.DataType.DataType

class DataCell(val value: Any, var dataType: DataType = DataType.None) {
    if (dataType == DataType.None) {
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
