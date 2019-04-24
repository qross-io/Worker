package io.qross.psql

import java.util
import java.util.{HashMap, List, Map}
import java.util.regex.{Matcher, Pattern}

import io.qross.core.DataCell
import io.qross.jdbc.DataSource
import io.qross.util.{Console, DataCell, Timer}

import scala.util.control.Breaks._
import scala.collection.mutable

class PSQL(val SQL: String) {

    private val $SET = Pattern.compile("^SET\\s+(\\$\\{?[a-z_][a-z0-9_]*}?(\\s*,\\s*\\$\\{?[a-z_][a-z0-9_]*}?)*\\s*):=", Pattern.CASE_INSENSITIVE)
    private val $NAME = Pattern.compile("^([a-z][a-z0-9_#]*)?\\s*:", Pattern.CASE_INSENSITIVE)
    private val $TRY = Pattern.compile("^TRY\\s+(.+?)\\s+?@", Pattern.CASE_INSENSITIVE)
    private val $IF = Pattern.compile("^IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE)
    private val $ELSE_IF = Pattern.compile("^ELSE? ?IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE)
    private val $ELSE = Pattern.compile("^ELSE", Pattern.CASE_INSENSITIVE)
    private val $END_IF = Pattern.compile("""^END\s*IF""", Pattern.CASE_INSENSITIVE)
    private val $FOR$SELECT = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(SELECT\\s+.+)\\s+LOOP", Pattern.CASE_INSENSITIVE)
    private val $FOR$TO = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)\\s+TO\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE)
    private val $FOR$IN = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)(\\s+DELIMITED\\s+BY\\s+(.+))?\\s+LOOP", Pattern.CASE_INSENSITIVE)
    private val $WHILE = Pattern.compile("^WHILE\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE)
    private val $END_LOOP = Pattern.compile("^END\\s*LOOP", Pattern.CASE_INSENSITIVE)
    private val $SPACE = Pattern.compile("""\s""")

    val CAPTIONS = Set[String]("INSERT", "UPDATE", "DELETE", "CREATE")

    var params = ""
    var cacheName = ""
    var cacheEnabled = false

    var root: Statement = this.createStatement("ROOT", "")
    var sentences: Array[String] = _
    var cursor: Int = 0

    //数据源
    private var ds: DataSource = _
    //返回类型
    private var resultType: String = "list"
    //结果集
    private val ALL = new mutable.LinkedHashMap[String, AnyRef]()

    //正在解析的控制语句
    private val PARSING = new mutable.ArrayStack[Statement]
    //正在执行的控制语句
    private val EXECUTING = new mutable.ArrayStack[Statement]
    //待关闭的控制语句
    private val TO_BE_CLOSE = new mutable.ArrayStack[Statement]
    //IF条件执行结果
    private val IF_BRANCHES = new mutable.ArrayStack[Boolean]
    //FOR语句循环项变量值
    private val FOR_VARIABLES = new mutable.ArrayStack[ForLoopVariables]

    def createStatement(caption: String, sentence: String, expressions: String*) = new Statement(this, caption, sentence, expressions: _*)

    def parse(): Unit = {
        root.sentence = SQL
        PARSING.push(root)

        sentences = SQL.split(";")
        while(cursor < sentences.length) {
            val sentence = sentences(cursor).trim
            if (sentence.nonEmpty) {
                parseStatement(sentence.replace("~u003b", ";"))
            }
            cursor += 1
        }

        PARSING.pop()

        if (PARSING.nonEmpty || TO_BE_CLOSE.nonEmpty) {
            throw new SQLParserException("Control statement hasn't closed: " + PARSING.last.sentence);
        }
    }

    def parseStatement(sentence: String): Unit = {

        var m: Matcher = null

        if ( {m = $IF.matcher(SQL); m}.find) {
            val $if: Statement = this.createStatement("IF", m.group(0), m.group(1))
            $if.isClosed = false
            PARSING.last.addStatement($if)

            //只进栈
            PARSING.push($if)
            //待关闭的控制语句
            TO_BE_CLOSE.push($if)

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim())
        }
        else if ({m = $ELSE_IF.matcher(SQL); m}.find) {
            val $elseIf: Statement = this.createStatement("ELSE_IF", m.group(0), m.group(1))
            if (PARSING.isEmpty || (!(PARSING.last.caption == "IF") && !(PARSING.last.caption == "ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.last.addStatement($elseIf)
            PARSING.push($elseIf)
            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim)
        }
        else if ({m = $ELSE.matcher(SQL); m}.find) {
            val $else: Statement = this.createStatement("ELSE", m.group(0))
            if (PARSING.isEmpty || (!(PARSING.last.caption == "IF") && !(PARSING.last.caption == "ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.last.addStatement($else)
            PARSING.push($else)
            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim)
        }
        else if ({m = $END_IF.matcher(SQL); m}.find) {
            //检查IF语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) throw new SQLParserException("Can't find IF clause: " + m.group)
            else if (!(TO_BE_CLOSE.last.caption == "IF")) throw new SQLParserException(TO_BE_CLOSE.last.caption + " hasn't closed: " + TO_BE_CLOSE.last.sentence)
            else {
                TO_BE_CLOSE.last.isClosed = true
                TO_BE_CLOSE.pop()
            }
            val $endIf: Statement = this.createStatement("END_IF", m.group(0))
            //只出栈
            PARSING.pop
            PARSING.last.addStatement($endIf)
        }
        else if ({m = $FOR$SELECT.matcher(SQL); m}.find) {
            val $for$select: Statement = this.createStatement("FOR_SELECT", m.group(0), m.group(1).trim, m.group(2).trim)
            $for$select.isClosed = false
            PARSING.last.addStatement($for$select)
            //只进栈
            PARSING.push($for$select)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$select)
            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim)
        }
        else if ({m = $FOR$TO.matcher(SQL); m}.find) {
            val $for$to: Statement = this.createStatement("FOR_TO", m.group(0), m.group(1).trim(), m.group(2).trim(), m.group(3).trim())
            $for$to.isClosed = false
            PARSING.last.addStatement($for$to)

            //只进栈
            PARSING.push($for$to)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$to)

            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length()).trim())
        }
        else if ({m = $FOR$IN.matcher(SQL); m}.find) {
            val $for: Statement = this.createStatement("FOR_IN", m.group(0), m.group(1).trim, m.group(2).trim, (if (m.group(4) != null) {
                    m.group(4)
                }
                else {
                    ","
                }).trim)
            $for.isClosed = false
            PARSING.last.addStatement($for)
            //只进栈
            PARSING.push($for)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for)
            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim)
        }
        else if ({m = $WHILE.matcher(SQL); m}.find) {
            val $while: Statement = this.createStatement("WHILE", m.group(0), m.group(1).trim)
            $while.isClosed = false
            PARSING.last.addStatement($while)
            //只进栈
            PARSING.push($while)
            //待关闭的控制语句
            TO_BE_CLOSE.push($while)
            //继续解析子语句
            parseStatement(SQL.substring(m.group(0).length).trim)
        }
        else if ({m = $END_LOOP.matcher(SQL); m}.find) {
            //检查FOR语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParserException("Can't find FOR or WHILE clause: " + m.group)
            }
            else if (!Set("FOR_SELECT", "FOR_IN", "FOR_TO" , "WHILE").contains(TO_BE_CLOSE.last.caption)) {
                throw new SQLParserException(TO_BE_CLOSE.last.caption + " hasn't closed: " + TO_BE_CLOSE.last.sentence)
            }
            else {
                TO_BE_CLOSE.last.isClosed = true
                TO_BE_CLOSE.pop()
            }
            val $endLoop: Statement = this.createStatement("END_LOOP", m.group(0))
            //只出栈
            PARSING.pop()
            PARSING.last.addStatement($endLoop)
        }
        else if ({m = $SET.matcher(SQL); m}.find) {
            val $set: Statement = this.createStatement("SET", SQL, m.group(1).trim, SQL.substring(SQL.indexOf(":=") + 2).trim)
            PARSING.last.addStatement($set)
        }
        else {
            var name = ""
            var resultType = "list"
            var sentence = SQL
            if ({m = $NAME.matcher(sentence); m}.find) {
                name = m.group(1)
                if (name.contains("#")) {
                    resultType = name.substring(name.indexOf("#") + 1)
                    name = name.substring(0, name.indexOf("#"))
                }
                sentence = sentence.substring(m.group(0).length).trim
            }
            var retry = ""
            if ({m = $TRY.matcher(sentence); m}.find) {
                retry = m.group(1).trim
                sentence = sentence.substring(m.group(0).length).trim
            }
            if (!sentence.isEmpty) {
                if ({m = $SPACE.matcher(sentence); m}.find) {
                    val caption = sentence.substring(0, sentence.indexOf(m.group)).toUpperCase
                    if (!CAPTIONS.contains(caption) && !(caption == "SELECT")) {
                        throw new SQLParserException("Unsupported SQL sentence: " + sentence)
                    }
                    val query = this.createStatement(caption, SQL, sentence, name, resultType, retry)
                    //不进栈不出栈，添加子语句到当前语句块
                    PARSING.last.addStatement(query)
                }
                else {
                    throw new SQLParserException("Unrecognized or unsupported SQL: " + sentence)
                }
            }
        }
    }

    def updateVariableValue(field: String, value: Any): Boolean = {
        var found = false
        breakable {
            for (i <- FOR_VARIABLES.indices) {
                if (FOR_VARIABLES(i).contains(field)) {
                    FOR_VARIABLES(i).set(field, value)
                    found = true
                    break
                }
            }
        }

        if (!found) {
            breakable {
                for (i <- EXECUTING.indices) {
                    if (EXECUTING(i).containsVariable(field)) {
                        EXECUTING(i).setVariable(field, value)
                        found = true
                        break
                    }
                }
            }
        }

        if (!found) {
            EXECUTING.last.setVariable(field, value)
        }

        found
    }

    def findVariableValue(field: String): Option[DataCell] = {

        var result: Option[DataCell] = None

        breakable {
            for (i <- FOR_VARIABLES.indices) {
                if (FOR_VARIABLES(i).contains(field)) {
                    result = Some(FOR_VARIABLES(i).get(field))
                    break
                }
            }
        }

        if (result.isEmpty) {
            breakable {
                for (i <- EXECUTING.indices) {
                    if (EXECUTING(i).containsVariable(field)) {
                        result = Some(EXECUTING(i).getVariable(field))
                        break
                    }
                }
            }
        }

        result
    }

    private def execute(statements: List[Statement]): Unit = {

        for (statement <- statements) {
            statement.caption match {
                case "IF" =>
                    if (statement.instance.asInstanceOf[IfElse].conditionGroup.evalAll(this.ds)) {
                        IF_BRANCHES.push(true)
                        EXECUTING.push(statement)
                        this.execute(statement.statements)
                    }
                    else IF_BRANCHES.push(false)
                    break //todo: break is not supported
                case "ELSE_IF" =>
                    if (!IF_BRANCHES.lastElement) if (statement.instance.asInstanceOf[IfElse].conditionGroup.evalAll(this.ds)) { //替换
                        IF_BRANCHES.pop
                        IF_BRANCHES.push(true)
                        EXECUTING.push(statement)
                        this.execute(statement.statements)
                    }
                    break //todo: break is not supported
                case "ELSE" =>
                    if (!IF_BRANCHES.lastElement) {
                        IF_BRANCHES.pop
                        IF_BRANCHES.push(true)
                        EXECUTING.push(statement)
                        this.execute(statement.statements)
                    }
                    break //todo: break is not supported
                case "END_IF" =>
                    //结束本次IF语句
                    if (IF_BRANCHES.lastElement) { //在IF成功时才会有语句块进行栈
                        EXECUTING.pop
                    }
                    IF_BRANCHES.pop
                    break //todo: break is not supported
                case "FOR_SELECT" =>
                    val selectMap: ForLoopVariables = statement.instance.asInstanceOf[ForSelectLoop].computeMap(this.ds)
                    //FOR_VARIABLES（入栈）
                    FOR_VARIABLES.push(selectMap)
                    EXECUTING.push(statement)
                    //根据loopMap遍历
                    while ( {
                        selectMap.hasNext
                    }) this.execute(statement.statements)
                    break //todo: break is not supported
                case "FOR_TO" =>
                    val toLoop: ForToLoop = statement.instance.asInstanceOf[ForToLoop]
                    this.updateVariableValue(toLoop.variable, toLoop.parseBegin)
                    EXECUTING.push(statement)
                    while ( {
                        toLoop.hasNext
                    }) {
                        this.execute(statement.statements)
                        this.updateVariableValue(toLoop.variable, Integer.valueOf(this.findVariableValue(toLoop.variable).value.asInstanceOf[String]) + 1)
                    }
                    break //todo: break is not supported
                case "FOR_IN" =>
                    val inMap: ForLoopVariables = statement.instance.asInstanceOf[ForInLoop].computeMap
                    FOR_VARIABLES.push(inMap)
                    EXECUTING.push(statement)
                    while ( {
                        inMap.hasNext
                    }) this.execute(statement.statements)
                    break //todo: break is not supported
                case "WHILE" =>
                    val whileLoop: WhileLoop = statement.instance.asInstanceOf[WhileLoop]
                    EXECUTING.push(statement)
                    while ( {
                        whileLoop.conditionGroup.evalAll(this.ds)
                    }) this.execute(statement.statements)
                    break //todo: break is not supported
                case "END_LOOP" =>
                    if ("FOR_SELECT,FOR_IN".contains(EXECUTING.lastElement.caption)) FOR_VARIABLES.pop
                    EXECUTING.pop
                    break //todo: break is not supported
                //赋值语句
                case "SET" =>
                    statement.instance.asInstanceOf[SetVariable].assign(this.ds)
                    break //todo: break is not supported
                //执行语句
                case _ =>
                    val query: QuerySentence = statement.instance.asInstanceOf[QuerySentence]
                    //执行语句的功能不能放到子对象QuerySentence中，返回类型不好处理
                    val sentence: String = statement.parseQuerySentence(query.sentence)
                    Console.writeLine(sentence)
                    if (statement.caption == "SELECT") {
                        var rows: util.List[util.Map[String, AnyRef]] = ds.executeMapList(sentence)
                        if (rows.size == 0 && query.retry > -1) {
                            var retry: Int = 0
                            while ( {
                                rows.size == 0 && (query.retry == 0 || retry < query.retry)
                            }) {
                                Timer.sleep(0.1F)
                                retry += 1
                                rows = this.ds.executeMapList(sentence)
                            }
                        }
                        this.resultType match {
                            case "all" =>
                                if (!query.name.isEmpty) if (query.resultType.equalsIgnoreCase("single")) if (rows.size > 0 && rows.get(0).size > 0) {
                                        import scala.collection.JavaConversions._
                                        for (key <- rows.get(0).keySet) {
                                            ALL.put(query.name, rows.get(0).get(key))
                                            break //todo: break is not supported
                                        }
                                    }
                                    else ALL.put(query.name, "")
                                else if (query.resultType.equalsIgnoreCase("map")) ALL.put(query.name, if (rows.size > 0) rows.get(0)
                                        else new HashMap[AnyRef, AnyRef])
                                    else ALL.put(query.name, rows) //list
                                break //todo: break is not supported
                            case "list" =>
                                ALL.put("list", rows)
                                break //todo: break is not supported
                            case "map" =>
                                ALL.put("map", if (rows.size > 0) rows.get(0)
                                else new HashMap[AnyRef, AnyRef])
                                break //todo: break is not supported
                            case "single" =>
                                if (rows.size > 0 && rows.get(0).size > 0) {
                                    import scala.collection.JavaConversions._
                                    for (key <- rows.get(0).keySet) {
                                        ALL.put("single", rows.get(0).get(key))
                                        break //todo: break is not supported
                                    }
                                }
                                else ALL.put("single", "")
                                break //todo: break is not supported
                        }
                    }
                    else {
                        var affected: Int = this.ds.executeUpdate(sentence)
                        if (affected == 0 && query.retry > -1) {
                            var retry: Int = 0
                            while ( {
                                affected == 0 && (query.retry == 0 || retry < query.retry)
                            }) {
                                Timer.sleep(0.1F)
                                retry += 1
                                affected = this.ds.executeUpdate(sentence)
                            }
                        }
                        if (this.resultType == "none") ALL.put("none", affected)
                        else if (!query.name.isEmpty) ALL.put(query.name, affected)
                    }
                    break //todo: break is not supported
            }
        }
    }
}