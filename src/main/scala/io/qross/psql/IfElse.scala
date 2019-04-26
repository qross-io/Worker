package io.qross.psql

class IfElse(statement: Statement, conditions: String) {
    val conditionGroup = new ConditionGroup(statement, conditions)
}
