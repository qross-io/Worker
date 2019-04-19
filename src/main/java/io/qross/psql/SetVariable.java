package io.qross.psql;

import io.qross.util.DataSource;

import java.util.regex.Pattern;

public class SetVariable {

    public Statement statement;
    public String variable;
    public String expression;

    private static final Pattern $SELECT = Pattern.compile("^SELECT\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern $NON_QUERY = Pattern.compile("^(INSERT|UPDATE|DELETE)\\s", Pattern.CASE_INSENSITIVE);

    public SetVariable(Statement statement, String variables, String expression) {
        this.statement = statement;
        this.variable = Statement.removeVariableModifier(variables);
        this.expression = expression;

        if (variables.contains(",")) {
            if (!$SELECT.matcher(expression).find()) {
                throw new SQLParserException("Multiple variables definition only support SELECT sentence. " + expression);
            }
        }
    }

    //.forEach(this::updateVariableValue)
    public void assign(DataSource ds) {

        //1. SELECT查询  - 以SELECT开头 - 需要解析$开头的变量和函数
        //2. 非SELECT查询 - 以INSERT,UPDATE,DELETE开头 - 需要解析$开头的变量和函数

        //3. 字符串赋值或连接 - 用双引号包含，变量和内部函数需要加$前缀 - 需要解析$开头的变量和函数
        //4. 执行函数 - 以函数名开头，不需要加$前缀 - 直接执行函数
        //5. 变量间直接赋值 - 是变量格式(有$前缀)且是存在于变量列表中的变量 - 直接赋值
        //6. 数学表达式 - 其他 - 解析函数和变量然后求值，出错则抛出异常

        String expression = this.expression;

        if ($SELECT.matcher(expression).find()) {
            //SELECT
            if (expression.contains("$")) {
                expression = statement.parseQuerySentence(expression);
            }

            String[] names = variable.split(",");
            Object[] values = ds.executeDataRow(expression).getValues();
            for (int i = 0; i < names.length; i++) {
                //vars.put(names[i].trim(), i < row.size() ? (row.get(i) != null ? row.get(i).toString() : "null") : ""); 旧代码，这里出过NULL bug
                //this.statement.PSQL.updateVariableValue(names[i], row.contains(names[i]) ? row.getObject(names[i]) : "null"); 旧代码，这里出过null bug
                this.statement.PSQL.updateVariableValue(names[i].trim(), i < values.length ? values[i] : "null");
            }
        }
        else if ($NON_QUERY.matcher(expression).find()) {
            //INSERT + UPDATE + DELETE
            if (expression.contains("$")) {
                expression = statement.parseQuerySentence(expression);
            }

            this.statement.PSQL.updateVariableValue(variable, ds.executeUpdate(expression));
        }
        else {
            //其他，以标准表达式处理
            this.statement.PSQL.updateVariableValue(variable, statement.parseStandardSentence(expression));
        }
    }
}
