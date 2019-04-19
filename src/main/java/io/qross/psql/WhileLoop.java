package io.qross.psql;

//WhileLoop(String condition);
public class WhileLoop {

    public Statement statement = null;
    public ConditionGroup conditionGroup = null;

    public WhileLoop(Statement statement, String conditions) {
        this.conditionGroup = new ConditionGroup(statement, conditions);
        this.statement = statement;
        this.conditionGroup.statement = statement;
    }
}
