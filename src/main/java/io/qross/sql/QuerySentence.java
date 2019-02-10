package io.qross.sql;

public class QuerySentence {

    public Statement statement = null;

    public String name;
    public String resultType; //整个接口返回类型为all类型时单个SELECT的返回类型，必须指定name
    public int retry = -1; // -1 no retry, 0 limitless retry, >0 limit retry
    public String sentence; //可执行的语句主体

    public QuerySentence(Statement statement, String sentence, String name, String resultType, String retry) {
        this.statement = statement;
        this.sentence = sentence;
        this.name = name;
        this.resultType = resultType;
        if (!retry.isEmpty()) {
            this.retry = getRetryLimit(retry);
        }
    }

    private int getRetryLimit(String retryTimes) {

        String retry = retryTimes.toUpperCase();
        retry = retry.replace("ALWAYS", "0");

        if (!retry.matches("^\\d+$")) {
            throw new SQLParserException("Wrong TRY times expression: " + retryTimes + ", only supports integer or keyword ALWAYS");
        }

        return Integer.valueOf(retry);
    }
}
