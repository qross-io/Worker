package io.qross.util

class Cube(max: Int = -1) {

    private var value = -1
    private var closed = false

    def sum(v: Int = 1): Int = synchronized {
        this.value += v
        this.value
    }

    def get: Int = this.value

    def achieve(): Unit = synchronized {
        this.value = this.max
    }

    def isAchieved: Boolean = this.value >= this.max

    def close(): Unit = synchronized {
        this.closed = true
    }

    def isClosed: Boolean = this.closed
}
