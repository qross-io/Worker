package io.qross.psql

import java.util.regex.Matcher

import io.qross.core.{DataCell, DataHub, DataRow}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.psql.Patterns._
import io.qross.setting.Properties

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.Breaks._

object PSQL {

    def parse(SQL: String) = new PSQL(SQL)
}

class PSQL(var SQL: String) {

    private var params = ""
    private var cacheName = ""
    private var cacheEnabled = false
    var userId: Int = 0
    var userName: String = ""

    private val root: Statement = new Statement("ROOT", SQL)
    private var m: Matcher = _

    private var dh: DataHub = _
    //结果集
    private val ALL = new mutable.LinkedHashMap[String, Any]()

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

    //解析器
    private val PARSER = Map[String, String => Unit](
        "IF" ->  parseIF,
                "ELSE" -> parseELSE,
                "ELSIF" -> parseELSE,
                "END" -> parseEND,
                "FOR" -> parseFOR,
                "WHILE" -> parseWHILE,
                "SET" -> parseSET,
                "OPEN" -> parseOPEN,
                "SAVE" -> parseSAVE,
                "CACHE" -> parseCACHE,
                "TEMP" -> parseTEMP,
                "GET" -> parseGET,
                "PASS" -> parsePASS,
                "PUT" -> parsePUT,
                "OUT" -> parseOUT,
                "PRINT" -> parsePRINT,
                "LIST" -> parseLIST,
                "SELECT" -> parseSELECT
    )

    //执行器
    private val EXECUTOR = Map[String, Statement => Unit](
        "IF" -> executeIF,
        "ELSE_IF" -> executeELSE_IF,
        "ELSE" -> executeELSE,
        "END_IF" -> executeEND_IF,
        "FOR_SELECT" -> executeFOR_SELECT,
        "FOR_TO" -> executeFOR_TO,
        "FOR_IN" -> executeFOR_IN,
        "WHILE" -> executeWHILE,
        "END_LOOP" -> executeEND_LOOP,
        "SET" -> executeSET,
        "OPEN" -> executeOPEN,
        "SAVE" -> executeSAVE,
        "CACHE" -> executeCACHE,
        "TEMP" -> executeTEMP,
        "GET" -> executeGET,
        "PASS" -> executePASS,
        "PUT" -> executePUT,
        "OUT" -> executeOUT,
        "LIST" -> executeLIST,
        "PRINT" -> executePRINT,
        "SELECT" -> executeSELECT
    )

    //开始解析
    private def parseAll(): Unit = {

        PARSING.push(root)

        val sentences = SQL.split(";")
        for (sentence <- sentences) {
            if (sentence.trim.nonEmpty) {
                parseStatement(sentence.replace("~u003b", ";"))
            }
        }

        PARSING.pop()

        if (PARSING.nonEmpty || TO_BE_CLOSE.nonEmpty) {
            throw new SQLParserException("Control statement hasn't closed: " + PARSING.last.sentence)
        }
    }

    //解析入口
    def parseStatement(sentence: String): Unit = {
        val caption = sentence.trim.takeBefore("""\s""".r).toUpperCase
        if (PARSER.contains(caption)) {
            PARSER(caption)(sentence)
        }
        else if (Set("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE", "ALTER").contains(caption)) {
            PARSING.last.addStatement(new Statement(caption, sentence))
        }
        else {
            throw new SQLParserException("Unrecognized or unsupported sentence: " + sentence)
        }
    }

    private def parseIF(sentence: String): Unit = {
        if ({m = $IF.matcher(sentence); m}.find) {
            val $if: Statement = new Statement("IF", m.group(0), new ConditionGroup(m.group(1)))
            PARSING.last.addStatement($if)
            //只进栈
            PARSING.push($if)
            //待关闭的控制语句
            TO_BE_CLOSE.push($if)
            //继续解析第一条子语句
            parseStatement(sentence.substring(m.group(0).length).trim())
        }
        else {
            throw new SQLParserException("Incorrect IF sentence: " + sentence)
        }
    }

    private def parseELSE(sentence: String): Unit = {
        if ({m = $ELSE_IF.matcher(sentence); m}.find) {
            val $elsif: Statement = new Statement("ELSE_IF", m.group(0), new ConditionGroup(m.group(1)))
            if (PARSING.isEmpty || (!(PARSING.last.caption == "IF") && !(PARSING.last.caption == "ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.last.addStatement($elsif)
            PARSING.push($elsif)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else if ({m = $ELSE.matcher(sentence); m}.find) {
            val $else: Statement = new Statement("ELSE")
            if (PARSING.isEmpty || (!(PARSING.last.caption == "IF") && !(PARSING.last.caption == "ELSE_IF"))) {
                throw new SQLParserException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.last.addStatement($else)
            PARSING.push($else)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParserException("Incorrect ELSE or ELSIF sentence: " + sentence)
        }
    }

    private def parseEND(sentence: String): Unit = {
        if ({m = $END_IF.matcher(sentence); m}.find) {
            //检查IF语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) throw new SQLParserException("Can't find IF clause: " + m.group)
            else if (!(TO_BE_CLOSE.last.caption == "IF")) {
                throw new SQLParserException(TO_BE_CLOSE.last.caption + " hasn't closed: " + TO_BE_CLOSE.last.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endIf: Statement = new Statement("END_IF")
            //只出栈
            PARSING.pop()
            PARSING.last.addStatement($endIf)
        }
        else if ({m = $END_LOOP.matcher(sentence); m}.find) {
            //检查FOR语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParserException("Can't find FOR or WHILE clause: " + m.group)
            }
            else if (!Set("FOR_SELECT", "FOR_IN", "FOR_TO" , "WHILE").contains(TO_BE_CLOSE.last.caption)) {
                throw new SQLParserException(TO_BE_CLOSE.last.caption + " hasn't closed: " + TO_BE_CLOSE.last.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endLoop: Statement = new Statement("END_LOOP")
            //只出栈
            PARSING.pop()
            PARSING.last.addStatement($endLoop)
        }
        else {
            throw new SQLParserException("Incorrect END sentence: " + sentence)
        }
    }

    private def parseFOR(sentence: String): Unit = {
        if ({m = $FOR$SELECT.matcher(sentence); m}.find) {
            val $for$select: Statement = new Statement("FOR_SELECT", m.group(0), new FOR$SELECT(m.group(1).trim, m.group(2).trim))
            PARSING.last.addStatement($for$select)
            //只进栈
            PARSING.push($for$select)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$select)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else if ({m = $FOR$TO.matcher(sentence); m}.find) {
            val $for$to: Statement = new Statement("FOR_TO", m.group(0), new FOR$TO(m.group(1).trim().removeVariableModifier(), m.group(2).trim(), m.group(3).trim()))
            PARSING.last.addStatement($for$to)

            //只进栈
            PARSING.push($for$to)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$to)

            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length()).trim())
        }
        else if ({m = $FOR$IN.matcher(sentence); m}.find) {
            val $for: Statement = new Statement("FOR_IN", m.group(0),
                new FOR$IN(
                        m.group(1).trim,
                        m.group(2).trim,
                        (if (m.group(4) != null) {
                            m.group(4)
                        }
                        else {
                            ","
                        }).trim.removeQuotes()))
            PARSING.last.addStatement($for)
            //只进栈
            PARSING.push($for)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParserException("Incorrect FOR sentence: " + sentence)
        }
    }

    private def parseWHILE(sentence: String): Unit = {
        if ({m = $WHILE.matcher(sentence); m}.find) {
            val $while: Statement = new Statement("WHILE", m.group(0), new ConditionGroup(m.group(1).trim))
            PARSING.last.addStatement($while)
            //只进栈
            PARSING.push($while)
            //待关闭的控制语句
            TO_BE_CLOSE.push($while)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParserException("Incorrect WHILE sentence: " + sentence)
        }
    }

    private def parseSET(sentence: String): Unit = {
        if ({m = $SET.matcher(sentence); m}.find) {
            val $set: Statement = new Statement("SET", sentence, new SET(m.group(1).trim, sentence.substring(sentence.indexOf(":=") + 2).trim))
            PARSING.last.addStatement($set)
        }
        else {
            throw new SQLParserException("Incorrect SET sentence: " + sentence)
        }
    }

    private def parseOPEN(sentence: String): Unit = {
        if ({m = $OPEN.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("OPEN", sentence, new OPEN(m.group(1).trim, m.group(3).trim, m.group(5).trim)))
            if (m.group(1).endsWith(":")) {
                parseStatement(sentence.takeAfter(":"))
            }
        }
        else {
            throw new SQLParserException("Incorrect OPEN sentence: " + sentence)
        }
    }

    private def parseSAVE(sentence: String): Unit = {
        //save as
        if ({m = $SAVE_AS.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("SAVE", sentence, new SAVE(m.group(1), m.group(3))))
            if (m.group(1).endsWith(":")) {
                parseStatement(sentence.takeAfter(":"))
            }
        }
        else {
            throw new SQLParserException("Incorrect SAVE sentence: " + sentence)
        }
    }

    private def parseCACHE(sentence: String): Unit = {
        if ({m = $CACHE.matcher(sentence); m}.find) {
            val $cache = new Statement("CACHE", sentence.takeBefore("#"), new CACHE(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.last.addStatement($cache)
        }
        else {
            throw new SQLParserException("Incorrect CACHE sentence: " + sentence)
        }
    }

    private def parseTEMP(sentence: String): Unit = {
        if ({m = $TEMP.matcher(sentence); m}.find) {
            val $temp = new Statement("TEMP", sentence.takeBefore("#"), new TEMP(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.last.addStatement($temp)
        }
        else {
            throw new SQLParserException("Incorrect TEMP sentence: " + sentence)
        }
    }

    private def parseGET(sentence: String): Unit = {
        if ({m = $GET.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter("#"))))
        }
        else {
            throw new SQLParserException("Incorrect GET sentence: " + sentence)
        }
    }

    private def parsePASS(sentence: String): Unit = {
        if ({m = $PASS.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("PASS", sentence, new PASS(sentence.takeAfter("#"))))
        }
        else {
            throw new SQLParserException("Incorrect PASS sentence: " + sentence)
        }
    }

    private def parsePUT(sentence: String): Unit = {
        if ({m = $PUT.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("PUT", sentence, new PUT(sentence.takeAfter("#"))))
        }
        else {
            throw new SQLParserException("Incorrect PUT sentence: " + sentence)
        }
    }

    private def parseOUT(sentence: String): Unit = {
        if ({m = $OUT.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("OUT", sentence, new OUT(m.group(1), m.group(2), sentence.takeAfter("#").trim)))
        }
        else {
            throw new SQLParserException("Incorrect OUT sentence: " + sentence)
        }
    }

    private def parsePRINT(sentence: String): Unit = {
        if ({m = $PRINT.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("PRINT", sentence, new PRINT(m.group(1), m.group(2))))
        }
        else {
            throw new SQLParserException("Incorrect PRINT sentence: " + sentence)
        }
    }

    private def parseLIST(sentence: String): Unit = {
        if ({m = $LIST.matcher(sentence); m}.find) {
            PARSING.last.addStatement(new Statement("LIST", sentence, new LIST(Try(m.group(1).toInt).getOrElse(20))))
        }
        else {
            throw new SQLParserException("Incorrect LIST sentence: " + sentence)
        }
    }

    private def parseSELECT(sentence: String): Unit = {
        PARSING.last.addStatement(new Statement("SELECT", sentence))
    }

    private def executeIF(statement: Statement): Unit = {
        if (statement.instance.asInstanceOf[ConditionGroup].evalAll(statement, this.dh)) {
            IF_BRANCHES.push(true)
            EXECUTING.push(statement)
            this.execute(statement.statements)
        }
        else {
            IF_BRANCHES.push(false)
        }
    }

    private def executeELSE_IF(statement: Statement): Unit = {
        if (!IF_BRANCHES.last) {
            if (statement.instance.asInstanceOf[ConditionGroup].evalAll(statement, this.dh)) { //替换
                IF_BRANCHES.pop()
                IF_BRANCHES.push(true)
                EXECUTING.push(statement)

                this.execute(statement.statements)
            }
        }
    }

    private def executeELSE(statement: Statement): Unit = {
        if (!IF_BRANCHES.last) {
            IF_BRANCHES.pop()
            IF_BRANCHES.push(true)
            EXECUTING.push(statement)

            this.execute(statement.statements)
        }
    }

    private def executeEND_IF(statement: Statement): Unit = {
        //结束本次IF语句
        if (IF_BRANCHES.last) { //在IF成功时才会有语句块进行栈
            EXECUTING.pop()
        }
        IF_BRANCHES.pop()
    }

    private def executeFOR_SELECT(statement: Statement): Unit = {
        val selectMap: ForLoopVariables = statement.instance.asInstanceOf[FOR$SELECT].computeMap(this.dh)
        //FOR_VARIABLES（入栈）
        FOR_VARIABLES.push(selectMap)
        EXECUTING.push(statement)
        //根据loopMap遍历/
        while (selectMap.hasNext) {
            this.execute(statement.statements)
        }
    }

    private def executeFOR_TO(statement: Statement): Unit = {
        val toLoop: FOR$TO = statement.instance.asInstanceOf[FOR$TO]
        this.updateVariableValue(toLoop.variable, toLoop.parseBegin(statement))
        EXECUTING.push(statement)
        while (toLoop.hasNext(this, statement)) {
            this.execute(statement.statements)
            this.updateVariableValue(toLoop.variable, this.findVariableValue(toLoop.variable).value.asInstanceOf[String].toInt + 1)
        }
    }

    private def executeFOR_IN(statement: Statement): Unit = {
        val inMap: ForLoopVariables = statement.instance.asInstanceOf[FOR$IN].computeMap(statement)
        FOR_VARIABLES.push(inMap)
        EXECUTING.push(statement)
        while (inMap.hasNext) {
            this.execute(statement.statements)
        }
    }

    private def executeWHILE(statement: Statement): Unit = {
        val whileCondition: ConditionGroup = statement.instance.asInstanceOf[ConditionGroup]
        EXECUTING.push(statement)
        while (whileCondition.evalAll(statement, this.dh)) {
            this.execute(statement.statements)
        }
    }

    private def executeEND_LOOP(statement: Statement): Unit = {
        if (Set("FOR_SELECT", "FOR_IN").contains(EXECUTING.last.caption)) {
            FOR_VARIABLES.pop()
        }
        EXECUTING.pop()
    }

    private def executeSET(statement: Statement): Unit = {
        statement.instance.asInstanceOf[SET].assign(this, statement, this.dh)
    }

    private def executeOPEN(statement: Statement): Unit = {
        val $open = statement.instance.asInstanceOf[OPEN]
        $open.source match {
            case "CACHE" => dh.openCache()
            case "TEMP" => dh.openTemp()
            case "DEFAULT" => dh.openDefault()
            case _ => dh.open($open.source, $open.use)
        }
    }

    private def executeSAVE(statement: Statement): Unit = {
        val $save = statement.instance.asInstanceOf[SAVE]
        $save.targetType match {
            case "CACHE TABLE" => dh.cache($save.target)
            case "TEMP TABLE" => dh.temp($save.target)
            case _ =>
                $save.target match {
                    case "CACHE" => dh.saveAsCache()
                    case "TEMP" => dh.saveAsTemp()
                    case "DEFAULT" => dh.saveAsDefault()
                    case _ => dh.saveAs($save.target, $save.use)
                }
        }
    }

    private def executeCACHE(statement: Statement): Unit = {
        val $cache = statement.instance.asInstanceOf[CACHE]
        dh.get($cache.selectSQL).cache($cache.tableName)
    }

    private def executeTEMP(statement: Statement): Unit = {
        val $temp = statement.instance.asInstanceOf[TEMP]
        dh.get($temp.selectSQL).temp($temp.tableName)
    }

    private def executeGET(statement: Statement): Unit = {
        val $get = statement.instance.asInstanceOf[GET]
        dh.get($get.selectSQL)
    }

    private def executePASS(statement: Statement): Unit = {
        val $pass = statement.instance.asInstanceOf[PASS]
        dh.pass($pass.selectSQL)
    }

    private def executePUT(statement: Statement): Unit = {
        val $put = statement.instance.asInstanceOf[PUT]
        dh.put($put.nonQuerySQL)
    }

    private def executeOUT(statement: Statement): Unit = {
        val $out = statement.instance.asInstanceOf[OUT]
        $out.caption match {
            case "SELECT" =>
                val rows = dh.executeMapList($out.SQL)
                $out.outputType match {
                    case "SINGLE" =>
                        if (rows.nonEmpty && rows.head.nonEmpty) {
                            for (key <- rows.head.keySet) {
                                ALL.put($out.outputName, rows.head.get(key))
                            }
                        }
                        else {
                            ALL.put($out.outputName, "")
                        }
                    case "MAP" =>
                        ALL.put($out.outputName,
                                if (rows.nonEmpty) {
                                    rows.head
                                }
                                else {
                                    Map[String, Any]()
                                })
                    case "LIST" => ALL.put($out.outputName, rows)
                    case _ =>
                }
            case _ =>
                //非查询语句仅返回受影响的行数, 输出类型即使设置也无效
                ALL.put($out.outputName, this.dh.executeNonQuery($out.SQL))
        }
    }

    private def executePRINT(statement: Statement): Unit = {
        val $print = statement.instance.asInstanceOf[PRINT]
        $print.messageType match {
            case "WARN" => Output.writeWarning($print.message)
            case "ERROR" => Output.writeException($print.message)
            case "DEBUG" => Output.writeDebugging($print.message)
            case "INFO" => Output.writeDebugging($print.message)
            case "NONE" => Output.writeLine($print.message)
            case seal: String => Output.writeLineWithSeal(seal, $print.message)
            case _ =>
        }
    }

    private def executeLIST(statement: Statement): Unit = {
        val $list = statement.instance.asInstanceOf[LIST]
        dh.show($list.rows)
    }

    private def executeSELECT(statement: Statement): Unit = {
        ALL.put("LIST", dh.executeMapList(statement.sentence))
    }

    private def execute(statements: ArrayBuffer[Statement]): Unit = {

        for (statement <- statements) {
            if (EXECUTOR.contains(statement.caption)) {
                EXECUTOR(statement.caption)(statement)
            }
            else {
                ALL.put("AFFECTED", dh.executeNonQuery(statement.sentence))
            }
        }
    }

    //程序变量相关
    //root块存储程序级全局变量
    //其他局部变量在子块中
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

    def findVariableValue(field: String): DataCell = {

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

        result.orNull
    }

    //公开方法
    def $with(params: Map[String, Array[String]], defaultValue: String = ""): PSQL = {

        for (paramName <- params.keySet) {
            val paramValue = params(paramName)(0)
            //            try {
            //                paramValue = URLDecoder.decode(paramValue, "utf-8");
            //            } catch (UnsupportedEncodingException e) {
            //                e.printStackTrace();
            //            }
            if (this.SQL.contains("#{" + paramName + "}")) {
                if (this.params != "") {
                    this.params += "&"
                }
                this.params += paramName + "=" + paramValue
                this.SQL = this.SQL.replace("#{" + paramName + "}", paramValue)
            }
        }
        if (this.SQL.contains("#{")) {
            if (defaultValue.nonEmpty) {
                for ((paramName, paramValue) <- defaultValue.toHashMap()) {
                    this.SQL = this.SQL.replace("#{" + paramName + "}", paramValue)
                }
            }
        }
        this
    }

    def $with(params: Map[String, String]): PSQL = {

        for ((paramName, paramValue) <- params) {
            if (this.SQL.contains("#{" + paramName + "}")) {
                if (this.params != "") {
                    this.params += "&"
                }
                this.params += paramName + "=" + paramValue
                this.SQL = this.SQL.replace("#{" + paramName + "}", paramValue)
            }
        }
        this
    }

    def cache(name: String, cacheEnabled: Boolean): PSQL = {
        this.cacheName = name
        this.cacheEnabled = cacheEnabled
        this
    }

    def signIn(userId: Int, userName: String): PSQL = {
        this.userId = userId
        this.userName = userName

        //从数据库加载全局变量
        if (JDBC.hasQrossSystem) {
            // USER:NAME -> VALUE
            DataSource.queryDataTable("SELECT var_name, var_type, var_value FROM qross_variables WHERE var_group='USER' AND A.var_user=?", userId)
                    .foreach(row => {
                        GlobalVariable.USER.set(
                            row.getString("var_name"),
                            row.getString("var_type") match {
                                case "INTEGER" => row.getLong("var_value")
                                case "DECIMAL" => row.getDouble("var_value")
                                case _ => row.getString("var_value")
                            })

                    }).clear()
        }

        this
    }

    //设置单个变量的值
    def set(globalVariableName: String, value: String): PSQL = {
        root.setVariable(globalVariableName, value)
        this
    }

    def executeOn(ds: DataSource): PSQL = {
        this.dh = new DataHub()
        this.dh.open(ds)
        this
    }

    def andReturn(resultType: String = "list"): Any = {
        if (this.cacheEnabled && ResultCache.contains(this.cacheName, this.params)) {
            Output.writeLine("# FROM CACHE # " + this.SQL)
            ResultCache.get(this.cacheName, this.params)
        }
        else {
            this.parseAll()
            Output.writeLine("# FROM DATASOURCE # " + this.SQL)
            EXECUTING.push(root)
            this.execute(root.statements)
            dh.close()

            if (resultType.toLowerCase() == "all") {
                if (cacheEnabled) {
                    ResultCache.set(cacheName, params, ALL)
                }
                ALL
            }
            else {
                if (cacheEnabled) {
                    ResultCache.set(cacheName, params, ALL.get(resultType.toLowerCase()))
                }
                ALL.get(resultType.toLowerCase())
            }
        }
    }

    def show(): Unit = {
        val sentences = SQL.split(";")
        for (i <- sentences.indices) {
            Output.writeLine(i, ": ", sentences(i))
        }
        Output.writeLine("------------------------------------------------------------")
        this.root.show(0)
    }
}