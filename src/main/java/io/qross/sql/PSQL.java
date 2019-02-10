package io.qross.sql;

import io.qross.util.Timer;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PSQL {

    // PI = Procedure Interface
    // P = Procedure

    /*
    IF #{key} #= ^[0-9]+$ THEN
        SELECT * FROM qross_users WHERE id=#{key} OR POSITION('#{key}' IN mobile)=1;
        IF NOT condition THEN
            INSERT INTO abc (a) VALUES (1);
        END IF;
    ELSE IF #{key} #= ^[a-zA-Z]$ THEN
        SELECT * FROM qross_users WHERE initial='#{key}';
    ELSE IF #{key} *= @ THEN
        SELECT * FROM qross_users WHERE POSITION('#{key}' IN email)=1;
    ELSE
        SELECT * FROM qross_users WHERE role='#{key}' OR POSITION('#{key}' IN username)=1 OR POSITION('#{key}' IN fullname)=1;
    END IF;

    FOR ${eventName} IN #{eventName}
        LOOP
            INSERT INTO qross_jobs_events (job_id, event_name, event_function, event_value) SELECT  #{jobId}, '${eventName}', '#{eventFunction}', '' FROM dual WHERE NOT EXISTS (SELECT id FROM qross_jobs_events WHERE job_id=#{jobId} AND event_name='${eventName}' AND event_function='#{eventFunction}');
            UPDATE qross_jobs_events SET event_value=REPLACE(event_value, '#{eventValue}', '') WHERE job_id=#{jobId} AND event_name='${eventName}' AND event_function='SEND_MAIL_TO' AND '#{enabled}'='no';
            UPDATE qross_jobs_events SET event_value=CONCAT(event_value, '#{eventValue}') WHERE job_id=#{jobId} AND event_name='${eventName}' AND event_function='SEND_MAIL_TO' AND '#{enabled}'='yes' AND POSITION('#{eventValue}' IN event_value)=0;
        END LOOP;

    TRY ALWAYS @ SELECT * FROM table;

    */

    private static final Pattern $SET = Pattern.compile("^SET\\s+(\\$\\{?[a-z_][a-z0-9_]*}?(\\s*,\\s*\\$\\{?[a-z_][a-z0-9_]*}?)*\\s*):=", Pattern.CASE_INSENSITIVE);
    private static final Pattern $NAME = Pattern.compile("^([a-z][a-z0-9_#]*)?\\s*:", Pattern.CASE_INSENSITIVE);
    private static final Pattern $TRY = Pattern.compile("^TRY\\s+(.+?)\\s+?@", Pattern.CASE_INSENSITIVE);
    private static final Pattern $IF = Pattern.compile("^IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE);
    private static final Pattern $ELSE_IF = Pattern.compile("^ELSE? ?IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE);
    private static final Pattern $ELSE = Pattern.compile("^ELSE", Pattern.CASE_INSENSITIVE);
    private static final Pattern $END_IF = Pattern.compile("^END\\s*IF", Pattern.CASE_INSENSITIVE);
    private static final Pattern $FOR$SELECT = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(SELECT\\s+.+)\\s+LOOP", Pattern.CASE_INSENSITIVE);
    private static final Pattern $FOR$TO = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)\\s+TO\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE);
    private static final Pattern $FOR$IN = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)(\\s+DELIMITED\\s+BY\\s+(.+))?\\s+LOOP", Pattern.CASE_INSENSITIVE);
    private static final Pattern $WHILE = Pattern.compile("^WHILE\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE);
    private static final Pattern $END_LOOP = Pattern.compile("^END\\s*LOOP", Pattern.CASE_INSENSITIVE);
    private static final Pattern $SPACE = Pattern.compile("\\s");
    private static Set<String> CAPTIONS = new HashSet<String>() { {
        add("INSERT");
        add("UPDATE");
        add("DELETE");
        add("CREATE");
    } };

    public String SQL;
    public String params = "";
    public String cacheName = "";
    public boolean cacheEnabled = false;

    public Statement root = this.createStatement("ROOT", "");
    public String[] sentences;
    public int cursor = 0;

    //数据源
    private DataSource ds = null;
    //返回类型
    private String resultType = "list";
    //结果集
    private Map<String, Object> ALL = new LinkedHashMap<>();

    //正在解析的控制语句
    private Stack<Statement> PARSING = new Stack<>();
    //正在执行的控制语句
    private Stack<Statement> EXECUTING = new Stack<>();
    //待关闭的控制语句
    private Stack<Statement> TO_BE_CLOSE = new Stack<>();
    //IF条件执行结果
    private Stack<Boolean> IF_BRANCHES = new Stack<>();
    //FOR语句循环项变量值
    private Stack<ForLoopVariables> FOR_VARIABLES = new Stack<>();


    public PSQL(String SQLStatement) {
        this.SQL = SQLStatement;
    }

    public void parse() {
        root.sentence = this.SQL;
        PARSING.push(root);

        this.sentences = this.SQL.split(";");
        while (this.cursor < this.sentences.length) {
            String SQL = this.sentences[this.cursor].trim();
            if (!SQL.isEmpty()) {
                parseStatement(SQL.replace("~u003b", ";"));
            }
            this.cursor++;
        }

        PARSING.pop();

        if (PARSING.size() > 0 || TO_BE_CLOSE.size() > 0) {
            throw new SQLParserException("Control statement hasn't closed: " + PARSING.lastElement().sentence);
        }
    }

    public Statement createStatement(String caption, String sentence, String...expressions) {
        return new Statement(this, caption, sentence, expressions);
    }

    public void parseStatement(String SQL) {

        Matcher m;

        if ((m = $IF.matcher(SQL)).find()) {
            Statement $if = this.createStatement("IF", m.group(0), m.group(1));
            $if.isClosed = false;
            PARSING.lastElement().addStatement($if);

            //只进栈
            PARSING.push($if);
            //待关闭的控制语句
            TO_BE_CLOSE.push($if);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $ELSE_IF.matcher(SQL)).find()) {
            Statement $elseIf = this.createStatement("ELSE_IF", m.group(0), m.group(1));

            if (PARSING.isEmpty() || (!PARSING.lastElement().caption.equals("IF") && !PARSING.lastElement().caption.equals("ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0));
            }
            //先出栈再进栈
            PARSING.pop();
            PARSING.lastElement().addStatement($elseIf);
            PARSING.push($elseIf);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $ELSE.matcher(SQL)).find()) {
            Statement $else = this.createStatement("ELSE", m.group(0));

            if (PARSING.isEmpty() || (!PARSING.lastElement().caption.equals("IF") && !PARSING.lastElement().caption.equals("ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0));
            }

            //先出栈再进栈
            PARSING.pop();
            PARSING.lastElement().addStatement($else);
            PARSING.push($else);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $END_IF.matcher(SQL)).find()) {
            //检查IF语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty()) {
                throw new SQLParserException("Can't find IF clause: " + m.group());
            } else if (!TO_BE_CLOSE.lastElement().caption.equals("IF")) {
                throw new SQLParserException(TO_BE_CLOSE.lastElement().caption + " hasn't closed: " + TO_BE_CLOSE.lastElement().sentence);
            }
            else {
                TO_BE_CLOSE.lastElement().isClosed = true;
                TO_BE_CLOSE.pop();
            }

            Statement $endIf = this.createStatement("END_IF", m.group(0));
            //只出栈
            PARSING.pop();
            PARSING.lastElement().addStatement($endIf);
        }
        else if ((m = $FOR$SELECT.matcher(SQL)).find()) {
            Statement $for$select = this.createStatement("FOR_SELECT", m.group(0), m.group(1).trim(), m.group(2).trim());
            $for$select.isClosed = false;

            PARSING.lastElement().addStatement($for$select);

            //只进栈
            PARSING.push($for$select);
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$select);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $FOR$TO.matcher(SQL)).find()) {
            Statement $for$to = this.createStatement("FOR_TO", m.group(0), m.group(1).trim(), m.group(2).trim(), m.group(3).trim());
            $for$to.isClosed = false;
            PARSING.lastElement().addStatement($for$to);

            //只进栈
            PARSING.push($for$to);
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$to);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $FOR$IN.matcher(SQL)).find()) {
            Statement $for = this.createStatement("FOR_IN", m.group(0), m.group(1).trim(), m.group(2).trim(), (m.group(4) != null ? m.group(4) : ",").trim());
            $for.isClosed = false;
            PARSING.lastElement().addStatement($for);

            //只进栈
            PARSING.push($for);
            //待关闭的控制语句
            TO_BE_CLOSE.push($for);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $WHILE.matcher(SQL)).find()) {
            Statement $while = this.createStatement("WHILE", m.group(0), m.group(1).trim());
            $while.isClosed = false;

            PARSING.lastElement().addStatement($while);

            //只进栈
            PARSING.push($while);
            //待关闭的控制语句
            TO_BE_CLOSE.push($while);

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim());
        }
        else if ((m = $END_LOOP.matcher(SQL)).find()) {
            //检查FOR语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty()) {
                throw new SQLParserException("Can't find FOR or WHILE clause: " + m.group());
            }
            else if (!",FOR_SELECT,FOR_IN,FOR_TO,WHILE".contains("," + TO_BE_CLOSE.lastElement().caption)) {
                throw new SQLParserException(TO_BE_CLOSE.lastElement().caption + " hasn't closed: " + TO_BE_CLOSE.lastElement().sentence);
            }
            else {
                TO_BE_CLOSE.lastElement().isClosed = true;
                TO_BE_CLOSE.pop();
            }

            Statement $endLoop = this.createStatement("END_LOOP", m.group(0));

            //只出栈
            PARSING.pop();
            PARSING.lastElement().addStatement($endLoop);
        }
        else if ((m = $SET.matcher(SQL)).find()) {
            Statement $set = this.createStatement("SET", SQL, m.group(1).trim(), SQL.substring(SQL.indexOf(":=") + 2).trim());

            PARSING.lastElement().addStatement($set);
        }
        else {
            String name = "";
            String resultType = "list";
            String sentence = SQL;
            if ((m = $NAME.matcher(sentence)).find()) {
                name = m.group(1);
                if (name.contains("#")) {
                    resultType = name.substring(name.indexOf("#") + 1);
                    name = name.substring(0, name.indexOf("#"));
                }
                sentence = sentence.substring(m.group(0).length()).trim();
            }

            String retry = "";
            if ((m = $TRY.matcher(sentence)).find()) {
                retry = m.group(1).trim();
                sentence = sentence.substring(m.group(0).length()).trim();
            }

            if (!sentence.isEmpty()) {
                if ((m = $SPACE.matcher(sentence)).find()) {
                    String caption = sentence.substring(0, sentence.indexOf(m.group())).toUpperCase();
                    if (!CAPTIONS.contains(caption) && !caption.equals("SELECT")) {
                        throw new SQLParserException("Unsupported SQL sentence: "  + sentence);
                    }

                    Statement query = this.createStatement(caption, SQL, sentence, name, resultType, retry);

                    //不进栈不出栈，添加子语句到当前语句块
                    PARSING.lastElement().addStatement(query);
                }
                else {
                    throw new SQLParserException("Unrecognized or unsupported SQL: " + sentence);
                }
            }
        }
    }

    public boolean updateVariableValue(String field, Object value) {
        boolean found = false;
        for (int i = FOR_VARIABLES.size() - 1; i >= 0; i--) {
            if (FOR_VARIABLES.get(i).contains(field)) {
                FOR_VARIABLES.get(i).set(field, value);
                found = true;
            }
        }

        if (!found) {
            for (int i = EXECUTING.size() - 1; i >= 0; i--) {
                if (EXECUTING.get(i).containsVariable(field)) {
                    EXECUTING.get(i).setVariable(field, value);
                    found = true;
                }
            }
        }

        if (!found) {
            EXECUTING.lastElement().setVariable(field, value);
        }

        return found;
    }

    public DataCell findVariableValue(String field) {
        for (int i = FOR_VARIABLES.size() - 1; i >= 0; i--) {
            if (FOR_VARIABLES.get(i).contains(field)) {
                return FOR_VARIABLES.get(i).get(field);
            }
        }

        for (int i = EXECUTING.size() - 1; i >= 0; i--) {
            if (EXECUTING.get(i).containsVariable(field)) {
                return EXECUTING.get(i).getVariable(field);
            }
        }

        return null;
    }

    private void execute(List<Statement> statements) {

        for (Statement statement : statements) {

            switch (statement.caption) {
                case "IF":
                    if (((IfElse) statement.instance).conditionGroup.evalAll(this.ds)) {
                        IF_BRANCHES.push(true);
                        EXECUTING.push(statement);

                        this.execute(statement.statements);
                    }
                    else {
                        IF_BRANCHES.push(false);
                    }
                    break;
                case "ELSE_IF":
                    if (!IF_BRANCHES.lastElement()) {
                        if (((IfElse) statement.instance).conditionGroup.evalAll(this.ds)) {
                            //替换
                            IF_BRANCHES.pop();
                            IF_BRANCHES.push(true);
                            EXECUTING.push(statement);

                            this.execute(statement.statements);
                        }
                    }
                    break;
                case "ELSE":
                    if (!IF_BRANCHES.lastElement()) {
                        IF_BRANCHES.pop();
                        IF_BRANCHES.push(true);
                        EXECUTING.push(statement);

                        this.execute(statement.statements);
                    }
                    break;
                case "END_IF":
                    //结束本次IF语句
                    if (IF_BRANCHES.lastElement()) {
                        //在IF成功时才会有语句块进行栈
                        EXECUTING.pop();
                    }
                    IF_BRANCHES.pop();
                    break;
                case "FOR_SELECT":
                    ForLoopVariables selectMap = ((ForSelectLoop) statement.instance).computeMap(this.ds);
                    //FOR_VARIABLES（入栈）
                    FOR_VARIABLES.push(selectMap);
                    EXECUTING.push(statement);
                    //根据loopMap遍历
                    while (selectMap.hasNext()) {
                        this.execute(statement.statements);
                    }
                    break;
                case "FOR_TO":
                    ForToLoop toLoop = ((ForToLoop) statement.instance);
                    this.updateVariableValue(toLoop.variable, toLoop.parseBegin());

                    EXECUTING.push(statement);

                    while (toLoop.hasNext()) {
                        this.execute(statement.statements);
                        this.updateVariableValue(toLoop.variable, Integer.valueOf((String)this.findVariableValue(toLoop.variable).value) + 1);
                    }
                    break;
                case "FOR_IN":
                    ForLoopVariables inMap = ((ForInLoop) statement.instance).computeMap();
                    //FOR_VARIABLES（入栈）
                    FOR_VARIABLES.push(inMap);
                    EXECUTING.push(statement);
                    //根据loopMap遍历
                    while (inMap.hasNext()) {
                        this.execute(statement.statements);
                    }
                    break;
                case "WHILE":
                    WhileLoop whileLoop = ((WhileLoop) statement.instance);
                    //FOR_VARIABLES（入栈）
                    EXECUTING.push(statement);
                    while(whileLoop.conditionGroup.evalAll(this.ds)) {
                        this.execute(statement.statements);
                    }
                    break;
                case "END_LOOP":
                    if ("FOR_SELECT,FOR_IN".contains(EXECUTING.lastElement().caption)) {
                        FOR_VARIABLES.pop();
                    }
                    EXECUTING.pop();
                    break;
                //赋值语句
                case "SET":
                    ((SetVariable) statement.instance).assign(this.ds);
                    break;
                //执行语句
                default:
                    QuerySentence query = (QuerySentence) statement.instance;

                    //执行语句的功能不能放到子对象QuerySentence中，返回类型不好处理
                    String sentence = statement.parseQuerySentence(query.sentence);

                    Console.writeLine(sentence);

                    if (statement.caption.equals("SELECT")) {
                        List<Map<String, Object>> rows = ds.executeMapList(sentence);
                        if (rows.size() == 0 && query.retry > -1) {
                            int retry = 0;
                            while (rows.size() == 0 && (query.retry == 0 || retry < query.retry)) {
                                Timer.sleep(0.1F);
                                retry++;
                                rows = this.ds.executeMapList(sentence);
                            }
                        }

                        switch (this.resultType) {
                            case "all":
                                if (!query.name.isEmpty()) {
                                    if (query.resultType.equalsIgnoreCase("single")) {
                                        if (rows.size() > 0 && rows.get(0).size() > 0) {
                                            for (String key : rows.get(0).keySet()) {
                                                ALL.put(query.name, rows.get(0).get(key));
                                                break;
                                            }
                                        } else {
                                            ALL.put(query.name, "");
                                        }
                                    } else if (query.resultType.equalsIgnoreCase("map")) {
                                        ALL.put(query.name, (rows.size() > 0 ? rows.get(0) : new HashMap<>()));
                                    } else {
                                        ALL.put(query.name, rows); //list
                                    }
                                }
                                break;
                            case "list":
                                ALL.put("list", rows);
                                break;
                            case "map":
                                ALL.put("map", (rows.size() > 0 ? rows.get(0) : new HashMap<>()));
                                break;
                            case "single":
                                if (rows.size() > 0 && rows.get(0).size() > 0) {
                                    for (String key : rows.get(0).keySet()) {
                                        ALL.put("single", rows.get(0).get(key));
                                        break;
                                    }
                                } else {
                                    ALL.put("single", "");
                                }
                                break;
                        }
                    }
                    else {
                        int affected = this.ds.executeUpdate(sentence);
                        if (affected == 0 && query.retry > -1) {
                            int retry = 0;
                            while (affected == 0 && (query.retry == 0 || retry < query.retry)) {
                                Timer.sleep(0.1F);
                                retry++;
                                affected = this.ds.executeUpdate(sentence);
                            }
                        }

                        if (this.resultType.equals("none")) {
                            ALL.put("none", affected);
                        } else if (!query.name.isEmpty()) {
                            ALL.put(query.name, affected);
                        }
                    }

                    break;
            }
        }
    }

    public static PSQL parse(String SQL) {
        return new PSQL(SQL);
    }

    public PSQL with(Map<String, String[]> params, String defaultValue) {

        String paramValue;
        for (String paramName : params.keySet()) {
            paramValue = params.get(paramName)[0];
            if (this.SQL.contains("#{" + paramName + "}")) {
                if (!this.params.equals("")) {
                    this.params += "&";
                }
                this.params += paramName + "=" + paramValue;
                this.SQL = this.SQL.replace("#{" + paramName + "}", paramValue.replace("'", "''"));
            }
        }

        if (this.SQL.contains("#{")) {
            if (defaultValue != null && !defaultValue.isEmpty()) {
                Map<String, String> defaultValues = Common.parseQueryString(defaultValue);
                for (String paramName : defaultValues.keySet()) {
                    this.SQL = this.SQL.replace("#{" + paramName + "}", defaultValues.get(paramName).replace("'", "''"));
                }
            }
        }

        return this;
    }

    public PSQL with(Map<String, String> params) {
        for (String paramName : params.keySet()) {
            if (this.SQL.contains("#{" + paramName + "}")) {
                if (!this.params.equals("")) {
                    this.params += "&";
                }
                this.params += paramName + "=" + params.get(paramName);
                this.SQL = this.SQL.replace("#{" + paramName + "}", params.get(paramName).replace("'", "''"));
            }
        }

        return this;
    }

    public PSQL cache(String name, boolean cacheEnabled) {
        this.cacheName = name;
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public PSQL set(String globalVariableName, String value) {
        root.setVariable(globalVariableName, value);
        return this;
    }

    public PSQL executeOn(DataSource ds) {
        this.ds = ds;
        return this;
    }

    public Object andReturn(String resultType) {

        if (this.cacheEnabled && CACHE.contains(this.cacheName, this.params)) {

            Console.writeLine("# CACHE # " + this.SQL);

            return CACHE.get(this.cacheName, this.params);
        }
        else {
            this.parse();

            Console.writeLine("# DATASOURCE # " + this.SQL);

            //execute
            this.resultType = resultType.toLowerCase();
            EXECUTING.push(root);
            this.execute(root.statements);
            ds.close();

            if (this.resultType.equals("all")) {
                if (cacheEnabled) {
                    CACHE.set(cacheName, params, ALL);
                }
                return ALL;
            }
            else {
                if (cacheEnabled) {
                    CACHE.set(cacheName, params, ALL.get(this.resultType));
                }
                return ALL.get(this.resultType);
            }
        }
    }

    public void show() {

        for (int i = 0; i < this.sentences.length; i++) {
            Console.writeLine(i, ": ", this.sentences[i]);
        }

        Console.writeLine("------------------------------------------------------------");

        this.root.show(0);
    }
}