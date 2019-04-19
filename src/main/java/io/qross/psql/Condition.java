package io.qross.psql;

import io.qross.util.Common;
import io.qross.util.DataCell;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Condition {

    public String expression;
    public String field;
    public String operator;
    public String value; //or unary
    public boolean result = false;

    /*
    operator 列表

    等于 =, ==
    不等于 !=, <>
    开始于 ^=
    非开始于 =^
    结束于 $=
    非结束于 =$
    包含于 *=
    不包含于 =*
    正则表达式匹配 #=
    正则表达式不匹配 =#
    存在 EXISTS ()
    不存在 NOT EXISTS ()
    在列表中 IN ()
    不在列表中 NOT IN ()
    大于 >
    小于 <
    大于等于 >=
    小于等于 <=
    为NULL IS NULL
    不为NULL IS NOT NULL
    为空值 IS EMPTY
    不为空值 IS NOT EMPTY

    AND
    OR
    NOT
    */

    private static final Pattern $OPERATOR = Pattern.compile("==|!=|<>|>=|<=|\\^=|=\\^|\\$=|=\\$|\\*=|=\\*|#=|=#|>|<|=|\\sNOT\\s+IN\\s|\\sIS\\s+NOT\\s|\\sIN\\s|\\sIS\\s|\\sAND\\s|\\sOR\\s|^NOT\\s+EXISTS|^EXISTS|^NOT\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern $IS = Pattern.compile("^IS\\s", Pattern.CASE_INSENSITIVE);
    private static final Map<String, Boolean> $BOOLEANS = new HashMap<String, Boolean>() {{
        put("true", true);
        put("yes", true);
        put("ok", true);
        put("1", true);
        put("on", true);
        put("false", false);
        put("no", false);
        put("0", false);
        put("cancel", false);
        put("off", false);
    }};

    public Condition(String expression) {
        this.expression = expression;
        if ($IS.matcher(expression).find()) {
            expression = " " + expression;
        }
        Matcher m = $OPERATOR.matcher(expression);
        if (m.find()) {
            this.operator = m.group();
            this.field = expression.substring(0, expression.indexOf(this.operator)).trim();
            this.value = expression.substring(expression.indexOf(this.operator) + this.operator.length()).trim();
            this.operator = this.operator.trim().toUpperCase();
            this.operator = Pattern.compile("\\s").matcher(this.operator).replaceAll("");
        }
        else {
            this.operator = ""; //default
            //throw new SQLParserException("Wrong condition clause: " + expression);
        }
    }

    private boolean parseBoolean(String bool) {
        return  $BOOLEANS.getOrDefault(Statement.removeQuotes(bool.trim().toLowerCase()), false);
    }

    public void eval(String field, String value) {
        switch (this.operator) {
            case "AND":
                this.result = parseBoolean(field) && parseBoolean(value);
                break;
            case "OR":
                this.result = parseBoolean(field) || parseBoolean(value);
                break;
            case "NOT":
                this.result = !parseBoolean(value);
                break;
            case "EXISTS":
                this.result = !value.equals("()");
                break;
            case "NOTEXISTS":
                this.result = value.equals("()");
                break;
            case "IN":
                if (value.contains("'")) {
                    if (!field.startsWith("'")) {
                        field = "'" + field;
                    }
                    if (!field.endsWith("'")) {
                        field += "'";
                    }
                }
                value = value.trim();
                if (value.startsWith("(")) {
                    value = value.substring(1);
                }
                if (value.endsWith(")")) {
                    value = value.substring(0, value.length() - 1);
                }
                if (!value.startsWith(",")) {
                    value = "," + value;
                }
                if (!value.endsWith(",")) {
                    value += ",";
                }
                this.result = value.contains("," + field + ",");
                break;
            case "NOTIN":
                if (value.contains("'")) {
                    if (!field.startsWith("'")) {
                        field = "'" + field;
                    }
                    if (!field.endsWith("'")) {
                        field += "'";
                    }
                }
                value = value.trim();
                if (value.startsWith("(")) {
                    value = value.substring(1);
                }
                if (value.endsWith(")")) {
                    value = value.substring(0, value.length() - 1);
                }
                if (!value.startsWith(",")) {
                    value = "," + value;
                }
                if (!value.endsWith(",")) {
                    value += ",";
                }
                this.result = !value.contains("," + field + ",");
                break;
            case "IS":
                if (value.equalsIgnoreCase("NULL")) {
                    this.result = (field.contains("#{") || field.contains("${")) && field.contains("}") || field.equalsIgnoreCase("NULL");
                }
                else if (value.equalsIgnoreCase("EMPTY")) {
                    this.result = field.isEmpty() || field.equals("\"\"") || field.equals("''");
                }
                break;
            case "ISNOT":
                if (value.equalsIgnoreCase("NULL")) {
                    this.result = !field.contains("#{") && !field.contains("${") && !field.equalsIgnoreCase("NULL");
                }
                else if (value.equalsIgnoreCase("EMPTY")) {
                    this.result = !field.isEmpty() && !field.equals("\"\"") && !field.equals("''");
                }
                break;
            case "=":
            case "==":
                this.result = field.equalsIgnoreCase(value);
                break;
            case "!=":
            case "<>":
                this.result = !field.equalsIgnoreCase(value);
                break;
            case "^=":
                this.result = field.toLowerCase().startsWith(value.toLowerCase());
                break;
            case "=^":
                this.result = !field.toLowerCase().startsWith(value.toLowerCase());
                break;
            case "$=":
                this.result = field.toLowerCase().endsWith(value.toLowerCase());
                break;
            case "=$":
                this.result = !field.toLowerCase().endsWith(value.toLowerCase());
                break;
            case "*=":
                this.result = field.toLowerCase().contains(value.toLowerCase());
                break;
            case "=*":
                this.result = !field.toLowerCase().contains(value.toLowerCase());
                break;
            case "#=":
                this.result = Pattern.compile(value, Pattern.CASE_INSENSITIVE).matcher(field).find();
                break;
            case "=#":
                this.result = !Pattern.compile(value, Pattern.CASE_INSENSITIVE).matcher(field).find();
                break;
            case ">=":
                try {
                    this.result = Double.valueOf(field) >= Double.valueOf(value);
                }
                catch (Exception e) {
                    throw new SQLParserException("Value must be number on >= compare: " + field + " >= " + value);
                }
                break;
            case "<=":
                try {
                    this.result = Double.valueOf(field) <= Double.valueOf(value);
                }
                catch (Exception e) {
                    throw new SQLParserException("Value must be number on >= compare: " + field + " <= " + value);
                }
                break;
            case ">":
                try {
                    this.result = Double.valueOf(field) > Double.valueOf(value);
                }
                catch (Exception e) {
                    throw new SQLParserException("Value must be number on >= compare: " + field + " > " + value);
                }
                break;
            case "<":
                try {
                    this.result = Double.valueOf(field) < Double.valueOf(value);
                }
                catch (Exception e) {
                    throw new SQLParserException("Value must be number on >= compare: " + field + " < " + value);
                }
                break;
            default:
                DataCell result = Common.jsEval(this.expression);
                if (result != null) {
                    this.result = parseBoolean(result.value.toString());
                }
                else {
                    this.result = parseBoolean(this.expression);
                }
                break;
        }
    }
}