package io.qross.util

class Cube(max: Int = -1) {

    private var value = -1
    private var threads = 0

    def increase(v: Int = 1): Int = synchronized {
        this.value += v
        this.value
    }

    def get: Int = this.value

    def achieve(): Unit = synchronized {
        this.value = this.max
    }

    def isAchieved: Boolean = this.value >= this.max

    def close(): Unit = synchronized {
        this.value = -1
    }

    def mark(): Unit = synchronized {
        this.threads += 1
    }

    def wipe(): Unit = synchronized {
        this.threads -= 1
        if (this.threads == 0) {
            this.value = -1
        }
    }

    def isClosed: Boolean = this.value == -1 && this.threads == 0
}
