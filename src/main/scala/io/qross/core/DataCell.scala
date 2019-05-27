package io.qross.core

import io.qross.jdbc.DataType
import io.qross.jdbc.DataType.DataType

class DataCell(val value: Any, var dataType: DataType = DataType.NULL) {
    if (value != null && dataType == DataType.NULL) {
        dataType = DataType.of(value)
    }

    def isNull: Boolean = {
        value == null
    }

    def isNotNull: Boolean = {
        value != null
    }

    def data: Option[Any] = {
        Option(value)
    }

    def ifNotNull(handler: DataCell => Unit): DataCell = {
        if (isNotNull) {
            handler(this)
        }
        this
    }

    def ifNull(handler: () => Unit): DataCell = {
        if (isNull) {
            handler()
        }
        this
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
