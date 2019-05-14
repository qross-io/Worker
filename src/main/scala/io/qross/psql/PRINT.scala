package io.qross.psql

class PRINT(var messageType: String, val message: String) {
    if (messageType == null) {
        messageType = "NONE"
    }
}
