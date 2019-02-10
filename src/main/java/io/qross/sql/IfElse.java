package io.qross.sql;

public class IfElse {

    public Statement statement = null;
    public ConditionGroup conditionGroup = null;

    public IfElse(Statement statement, String conditions) {
        this.conditionGroup = new ConditionGroup(statement, conditions);
        this.statement = statement;
        this.conditionGroup.statement = statement;
    }
}
