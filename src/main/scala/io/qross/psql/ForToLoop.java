package io.qross.psql;

public class ForToLoop {

    public String variable; //自变量
    public String rangeBegin;
    public String rangeEnd;

    public Statement statement;

    public ForToLoop(Statement statement, String variable, String rangeBegin, String rangeEnd) {
        this.statement = statement;
        this.variable = Statement.removeVariableModifier(variable);
        this.rangeBegin = rangeBegin;
        this.rangeEnd = rangeEnd;
    }

    public int parseBegin() {
        return Integer.parseInt(this.statement.parseStandardSentence(this.rangeBegin));
    }

    private int parseEnd() {
        return Integer.parseInt(this.statement.parseStandardSentence(this.rangeEnd));
    }

    public boolean hasNext() {
        return Integer.parseInt((String)statement.PSQL.findVariableValue(this.variable).value) <= this.parseEnd();
    }
}
