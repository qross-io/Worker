package io.qross.psql;

import io.qross.util.Console;
import io.qross.util.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConditionGroup {

    public Statement statement;
    public String expression;
    private List<Condition> conditions = new ArrayList<>();
    private List<String> ins = new ArrayList<>();
    private List<String> exists = new ArrayList<>();
    private List<String> selects = new ArrayList<>();

    private static final String CONDITION = "#[condition:";
    private static final String N = "]";
    private static final Pattern $SELECT = Pattern.compile("\\(\\s*SELECT\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT$ = Pattern.compile("#\\[select:(\\d+?)]");
    private static final Pattern $EXISTS = Pattern.compile("EXISTS\\s*(\\([^)]+\\))", Pattern.CASE_INSENSITIVE);
    private static final Pattern EXISTS$ = Pattern.compile("#\\[exists:(\\d+?)]");
    private static final Pattern $IN = Pattern.compile("\\sIN\\s*(\\([^)]+\\))", Pattern.CASE_INSENSITIVE);
    private static final Pattern IN$ = Pattern.compile("#\\[in:(\\d+?)]");
    private static final Pattern $BRACKET = Pattern.compile("\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern $AND = Pattern.compile("(^|\\sOR\\s)((.+?)\\s+AND\\s+(.+?))($|\\sAND|\\sOR)", Pattern.CASE_INSENSITIVE);
    private static final Pattern $_OR_ = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern $OR = Pattern.compile("(^)((.+?)\\s+OR\\s+(.+?))(\\sOR|$)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONDITION$ = Pattern.compile("#\\[condition:(\\d+)]", Pattern.CASE_INSENSITIVE);

    public ConditionGroup(Statement statement, String expression) {
        this.statement = statement;
        this.expression = expression;
    }

    public boolean evalAll(DataSource ds) {

        Matcher m;
        //解析表达式
        String exp = this.statement.parseExpressions(this.expression);

        //replace SELECT to #[select:n]
        m = $SELECT.matcher(exp);
        while (m.find()) {
            String select = findOutSelect(exp, m.group());
            exp = exp.replace(select, "#[select:" + selects.size() + "]");
            selects.add(select);
        }

        //replace EXISTS () to #[exists:n]
        m = $EXISTS.matcher(exp);
        while (m.find()) {
            exp = exp.replace(m.group(1), "#[exists:" + exists.size() + "]");
            exists.add(m.group(1));
        }

        //replace IN () to #[in:n]
        m = $IN.matcher(exp);
        while (m.find()) {
            exp = exp.replace(m.group(1), "#[in:" + ins.size() + "]");
            ins.add(m.group(1));
        }

        //解析变量和函数
        exp = this.statement.parseVariablesAndFunctions(exp, true);

        // ()
        while ((m = $BRACKET.matcher(exp)).find()) {
            parseBasicExpression(m.group(1).trim());
            exp = exp.replace(m.group(0), CONDITION + (this.conditions.size() - 1) + N);
        }

        //finally
        parseBasicExpression(exp);

        //IN (SELECT ...)
        List<String> selectResult = new ArrayList<>();
        for (String select : selects) {
            List<String> list = ds.executeFirstColumnList(this.statement.parseVariablesAndFunctions(select, false));
            selectResult.add(String.join(",", list));
            //list.toArray(new String[list.size()])
            list.clear();
        }

        for (Condition condition : this.conditions) {

            String field = condition.field;
            String value = condition.value;

            m = SELECT$.matcher(value);
            while (m.find()) {
                value = value.replace(m.group(0), selectResult.get(Integer.valueOf(m.group(1))));
            }

            if ((m = CONDITION$.matcher(field)).find()) {
                field = String.valueOf(this.conditions.get(Integer.valueOf(m.group(1))).result);
            }
            else {
                if (!field.isEmpty()) {
                    field = statement.parseSingleExpression(field, false);
                }
            }

            if ((m = CONDITION$.matcher(value)).find()) {
                value = String.valueOf(this.conditions.get(Integer.valueOf(m.group(1))).result);
            }
            else {
                if (!value.equalsIgnoreCase("EMPTY") && !value.equalsIgnoreCase("NULL") && !value.equals("()")) {
                    value = statement.parseSingleExpression(value, false);
                }
            }

            condition.eval(field, value);

            Console.writeDotLine(" ", condition.field, condition.operator, condition.value, " => ", condition.result);
        }

        return conditions.get(conditions.size() - 1).result;
    }

    private String findOutSelect(String expression, String head) {

        int start = 0;
        int begin = expression.indexOf(head, start) + 1;
        int end = expression.indexOf(")", start);

        Stack<String> brackets = new Stack<>();
        brackets.push("(");
        start = begin;

        while(!brackets.isEmpty() && expression.indexOf(")", start) > -1) {
            int left = expression.indexOf("(", start);
            int right = expression.indexOf(")", start);
            if (left > -1 && left < right) {
                brackets.push("(");
                start = left + 1;
            }
            else {
                brackets.pop();
                start = right + 1;
                if (right > end) {
                    end = right;
                }
            }
        }

        if (!brackets.isEmpty()) {
            throw new SQLParserException("Can't find closed bracket for SELECT: " + expression);
        }
        else {
            return expression.substring(begin, end);
        }
    }

    //解析无括号的表达式
    public void parseBasicExpression(String expression) {

        Matcher m, n;
        String left;
        String right;
        String clause;

        //restore EXISTS
        m = EXISTS$.matcher(expression);
        while (m.find()) {
            expression = expression.replace(m.group(0), exists.get(Integer.valueOf(m.group(1))));
        }

        //restore IN
        m = IN$.matcher(expression);
        while (m.find()) {
            expression = expression.replace(m.group(0), ins.get(Integer.valueOf(m.group(1))));
        }

        //AND
        while ((m = $AND.matcher(expression)).find()) {

            clause = m.group(2);
            left = m.group(3);
            right = m.group(4);

            while ((n = $_OR_.matcher(clause)).find()) {
                clause = clause.substring(clause.indexOf(n.group()) + n.group().length());
                left = left.substring(left.indexOf(n.group()) + n.group().length());
            }

            if (!left.startsWith(CONDITION)) {
                expression = expression.replace(left, CONDITION + this.conditions.size() + N);
                clause = clause.replace(left, CONDITION + this.conditions.size() + N);
                this.conditions.add(new Condition(left.trim()));
            }

            if (!right.startsWith(CONDITION)) {
                expression = expression.replace(right, CONDITION + this.conditions.size() + N);
                clause = clause.replace(right, CONDITION + this.conditions.size() + N);
                this.conditions.add(new Condition(right.trim()));
            }

            expression = expression.replace(clause, CONDITION + this.conditions.size() + N);
            this.conditions.add(new Condition(clause.trim())); // left AND right
        }

        //OR
        while ((m = $OR.matcher(expression)).find()) {
            clause = m.group(2);
            left = m.group(3);
            right = m.group(4);

            if (!left.startsWith(CONDITION)) {
                expression = expression.replace(left, CONDITION + this.conditions.size() + N);
                clause = clause.replace(left, CONDITION + this.conditions.size() + N);
                this.conditions.add(new Condition(left.trim()));
            }

            if (!right.startsWith(CONDITION)) {
                expression = expression.replace(right, CONDITION + this.conditions.size() + N);
                clause = clause.replace(right, CONDITION + this.conditions.size() + N);
                this.conditions.add(new Condition(right.trim()));
            }

            expression = expression.replace(clause, CONDITION + this.conditions.size() + N);
            this.conditions.add(new Condition(clause.trim())); // left OR right
        }

        //SINGLE
        if (!expression.startsWith(CONDITION)) {
            this.conditions.add(new Condition(expression.trim()));
        }
    }
}