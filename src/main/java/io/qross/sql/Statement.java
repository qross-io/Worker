package io.qross.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Statement {

    public String caption;
    public Object instance; //与caption对应的实例对象
    public String sentence;

    public io.qross.sql.PSQL PSQL;
    //表示控制语句是否闭合, 用于解析检查
    public boolean isClosed = true;
    //所有子语句
    public List<Statement> statements = new ArrayList<>();
    //局部变量列表，对于root，则表示全局变量
    private DataRow variables = new DataRow();

    public static final Pattern $EXPRESSION = Pattern.compile("\\$\\{\\{(.+?)}}", Pattern.CASE_INSENSITIVE);
    public static final Pattern $FUNCTION = Pattern.compile("\\$\\{?(" + String.join("|", Function.NAMES) + ")\\s*\\(", Pattern.CASE_INSENSITIVE);
    public static final Pattern $VARIABLE = Pattern.compile("\\$\\{?([a-z_][a-z0-9_]*)}?", Pattern.CASE_INSENSITIVE);

    public Statement(io.qross.sql.PSQL PSQL, String caption, String sentence, String...expressions) {
        this.PSQL = PSQL;
        this.caption = caption;
        this.sentence = sentence;

        switch (caption) {
            case "ROOT":
                break;
            case "IF":
                this.instance = new IfElse(this, expressions[0]);
                break;
            case "ELSE_IF":
                this.instance = new IfElse(this, expressions[0]);
                break;
            case "ELSE":
                break;
            case "END_IF":
                break;
            case "FOR_SELECT":
                this.instance = new ForSelectLoop(this, expressions[0], expressions[1]);
                break;
            case "FOR_IN":
                this.instance = new ForInLoop(this, expressions[0], expressions[1], expressions[2]);
                break;
            case "FOR_TO":
                this.instance = new ForToLoop(this, expressions[0], expressions[1], expressions[2]);
                break;
            case "WHILE":
                this.instance = new WhileLoop(this, expressions[0]);
                break;
            case "END_LOOP":
                break;
            case "SET":
                this.instance = new SetVariable(this, expressions[0], expressions[1]);
                break;
            default:
                this.instance = new QuerySentence(this, expressions[0], expressions[1], expressions[2],  expressions[3]);
                break;
        }
    }

    public boolean containsVariable(String name) {
        return this.variables.contains(name);
    }

    public DataCell getVariable(String name) {
        return this.variables.getCell(name);
    }

    public void setVariable(String name, Object value) {
        this.variables.set(name, value);
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);
    }

    public void show(int level) {

        for (int i = 0; i < level; i++) {
            System.out.print("\t");
        }
        Console.writeLine(this.sentence);
        for (Statement statement : this.statements) {
            statement.show(level + 1);
        }
    }

    //为计算值添加双引号，用于计算过程中
    public static String useQuotes(String value) {
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

    //去掉常量中的双引号，用于计算结果
    public static String removeQuotes(String value) {
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1);
            value = value.substring(0, value.length() - 1);
            value = value.replace("\\\"", "\"");
        }
        return value;
    }

    public static String removeVariableModifier(String value) {
        return value.replace("$", "").replace("{", "").replace("}", "").trim();
    }

    //不能事先把变量替换成数值，因为可能之后这个变量的值可能会发生变化
    //在执行时才计算变量的值

    //解析查询语句中的表达式、函数和变量，均不保留双引号
    public String parseQuerySentence(String sentence) {
        //均不保留双引号
        // 表达式 ${{ expression }}
        sentence = parseExpressions(sentence);

        // 之前决定的是先解析函数再解析变量，忘了为啥了 2018.12.7

        // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        sentence = parseVariables(sentence, false);

        // 函数 ${FUNC()} $FUNC()
        sentence = parseFunctions(sentence, false);

        return sentence;
    }

    //解析标准表达式语句
    public String parseStandardSentence(String sentence) {

        sentence = parseExpressions(sentence);
        sentence = parseSingleExpression(sentence, false);

        return sentence;
    }

    //解析条件表达式语句
    public String parseVariablesAndFunctions(String sentence, boolean retainQuotes) {

        sentence = parseVariables(sentence, retainQuotes);
        sentence = parseFunctions(sentence, retainQuotes);

        return sentence;
    }

    //解析查询语句中的表达式
    public String parseExpressions(String sentence) {
        Matcher m = $EXPRESSION.matcher(sentence);
        while (m.find()) {
            sentence = sentence.replace(m.group(0), parseSingleExpression(m.group(1), false));
        }
        return sentence;
    }

    //解析完整的表达式 - SET表达式右侧 / 条件表达式左右侧 / 函数的每个参数 / FOR-IN 表达式右侧 / FOR-TO 表达式左右侧
    //解析过程中，字符串需要保留双引号
    public String parseSingleExpression(String expression, boolean retainQuotes) {

        // 先计算变量，再计算函数
        // 变量 ${VAR} $VAR
        expression = parseVariables(expression, true);
        // 函数 ${FUNC()} $FUNC()
        expression = parseFunctions(expression, true);
        // 以js方式执行表达式
        DataCell result = Common.jsEval(expression);
        if (result == null) {
            throw new SQLParserException("Can't calculate the expression: " + expression);
        }

        expression = result.value.toString();
        //如果是字符串，则加上引号
        if (retainQuotes && result.dataType == DataType.TEXT) {
            expression = useQuotes(expression);
        }

        return  expression;
    }

    //解析表达式中的函数
    public String parseFunctions(String expression, boolean retainQuotes) {

        Matcher m;
        String chr;
        int start = 0, end;

        while ((m = $FUNCTION.matcher(expression)).find(start)) {

            Stack<String> brackets = new Stack<>();
            brackets.push("(");
            start = expression.indexOf(m.group());
            end = start + m.group().length();

            while (!brackets.isEmpty()) {

                chr = expression.substring(end, end + 1);
                switch (chr) {
                    case "(":
                        if (!brackets.lastElement().equals("\"")) {
                            brackets.push("(");
                        }
                        break;
                    case ")":
                        if (!brackets.lastElement().equals("\"")) {
                            brackets.pop();
                        }
                        break;
                    case "\"":
                        if (brackets.lastElement().equals("\"")) {
                            brackets.pop();
                        }
                        else {
                            brackets.push("\"");
                        }
                        break;
                }

                //遇到转义符，前进一位
                end += (chr.equals("\\") ? 2 : 1);

                if (end >= expression.length()) {
                    break;
                }
            }

            //函数应该正确闭合
            if (!brackets.isEmpty()) {
                brackets.clear();
                throw new SQLParserException("Miss right bracket in internal function: " + m.group(0));
            }

            //取按开始点和结束点正确的函数
            String function = expression.substring(start, end);
            String replacement = new Function(this).execute(function);
            if (!retainQuotes) {
                replacement = removeQuotes(replacement);
            }
            start += replacement.length();

            expression = expression.replace(function, replacement);
        }

        return expression;
    }

    //解析表达式中的变量
    public String parseVariables(String expression, boolean retainQuotes) {

        String worked = "";
        Matcher m = $VARIABLE.matcher(expression);
        while (m.find()) {
            worked += expression.substring(0, expression.indexOf(m.group()));
            expression = expression.substring(expression.indexOf(m.group()) + m.group().length());

            DataCell result = this.PSQL.findVariableValue(m.group(1));
            String replacement = null;
            if (result != null) {
                replacement = result.value.toString();
                if (retainQuotes && result.dataType == DataType.TEXT) {
                    replacement = useQuotes(replacement);
                }
            }
            if (replacement != null) {
                worked += replacement;
            }
            else {
                worked += m.group();
            }
        }
        expression = worked + expression;

        return expression;
    }
}
