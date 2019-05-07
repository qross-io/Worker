package io.qross.psql

class WHILE(val statement: Statement, private val conditions: String) {

    var conditionGroup: ConditionGroup = new ConditionGroup(statement, conditions)

}
