package io.qross.psql

class WhileLoop(val statement: Statement, private val conditions: String) {

    var conditionGroup: ConditionGroup = new ConditionGroup(statement, conditions)

}
