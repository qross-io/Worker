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

    def achieved: Boolean = this.value >= this.max


    def reset(): Unit = synchronized {
        this.value = -1
    }

    //活跃线程+1
    def mark(): Unit = synchronized {
        this.threads += 1
    }

    //活跃线程-1
    def wipe(): Unit = synchronized {
        this.threads -= 1
    }

    //获取活跃线程数量
    def active: Int = this.threads

    //是否所有线程已经退出
    def closed: Boolean = this.threads == 0
}
