package io.qross.util

import io.qross.util.DataType.DataType

class DataCell(value: Any, var dataType: DataType = DataType.None) {
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
