package io.qross.sql;

import io.qross.util.Common;
import io.qross.util.DataCell;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Function {

    public static final String[] NAMES = {
            "CONCAT", "CHARINDEX", "INSTR", "POSITION", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "LOWER", "UPPER", "TRIM", "SPLIT", "LEN",
            "IFNULL", "NVL",
            "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE",
            "PERMISSION", "TEST"
    };

    private static final Pattern $FUNCTION = Pattern.compile("\\$(" + String.join("|", Function.NAMES) + ")\\s*\\(([^()]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern $STRING = Pattern.compile("#\\{s(\\d+)}", Pattern.CASE_INSENSITIVE);

    public Statement statement;
    private List<String> strings = new ArrayList<>();

    public Function(Statement statement) {
        this.statement = statement;
    }

    //$FUNC("c,a,b", , );
    public String execute(String function) {

        Matcher m;

        //子函数
        while ((m = $FUNCTION.matcher(function)).find()) {
            function = function.replace(m.group(), this.execute(m.group(1), m.group(2)));
        }

        return function;
    }

    public String execute(String functionName, String arguments) {

        functionName = functionName.trim().toUpperCase();

        Matcher m;
        //替换掉所有的字符串，替换为 #{sx}
        if (arguments.contains("\"")) {
            arguments = arguments.replace("\\\"", "\\&QUOT;");

            Pattern p = Pattern.compile("\"[^\"]+\"");
            m = p.matcher(arguments);
            while (m.find()) {
                arguments = arguments.replace(m.group(0), "#{s" + strings.size() + "}");
                strings.add(m.group(0).replace("\\&QUOT;", "\\\""));
            }

            arguments = arguments.replace("\\&QUOT;", "\\\"");
        }

        if (functionName.equals("POSITION")) {
            m = Pattern.compile("[\\s\"](IN)[\\s\"]", Pattern.CASE_INSENSITIVE).matcher(arguments);
            if (m.find()) {
                arguments = arguments.substring(0, arguments.indexOf(m.group())) + m.group().replace(m.group(1), ",") + arguments.substring(arguments.indexOf(m.group()) + m.group().length());
            }
        }

        //按逗号得到参数列表
        String[] args = arguments.split(",");
        for (int i = 0; i < args.length; i++) {
            m = $STRING.matcher(args[i]);
            while (m.find()) {
                args[i] = args[i].replace(m.group(0), strings.get(Integer.parseInt(m.group(1))));
                DataCell result = Common.jsEval(args[i]);
                if (result == null) {
                    throw new SQLParserException("Wrong function argument or expression: " + args[i]);
                }
                else {
                    //这里不带括号
                    args[i] = result.value.toString();
                }
            }
        }

        //按参数类型确定是否带括号
        switch (functionName) {
            case "CONCAT":
                return Statement.useQuotes(String.join("", args));
            case "CHARINDEX":
            case "POSITION":
                if (args.length == 2) {
                    return String.valueOf(args[1].indexOf(args[0]) + 1);
                }
                else {
                    throw new SQLParserException("Need 2 arguments, actual " + args.length);
                }
            case "INSTR":
                return String.valueOf(args[0].indexOf(args[1]) + 1);
            case "REPLACE":
                return Statement.useQuotes(args[0].replace(args[1], args[2]));
            case "PERMISSION":
                DataCell userName = this.statement.PSQL.findVariableValue("USERNAME");
                DataCell apiName = this.statement.PSQL.findVariableValue("API_NAME");
                if (userName != null && apiName != null) {
                    return "\"" + Statement.useQuotes(userName.value.toString() + "-" + apiName.value.toString()) + "\"";
                }
                else {
                    return "\"NO_USER\"";
                }
            case "TEST":
                return "\"TEST\"";
        }

        return "";
    }
}
