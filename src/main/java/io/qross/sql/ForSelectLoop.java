package io.qross.sql;

import io.qross.util.DataSource;

public class ForSelectLoop {

    public Statement statement;
    public String[] variables;
    public String selectSQL;

    public ForSelectLoop(Statement statement, String variables, String selectSQL) {
        this.statement = statement;
        this.variables = Statement.removeVariableModifier(variables).split(",");
        this.selectSQL = selectSQL;
    }

    public ForLoopVariables computeMap(DataSource ds) {

        ForLoopVariables variables = new ForLoopVariables();

        ds.executeDataTable(selectSQL).forEach(variables::addRow);

        return variables;
    }
}