package io.qross.sql

import java.util.regex.Matcher

import io.qross.core.{DataCell, DataHub}
import io.qross.ext.Console
import io.qross.ext.PlaceHolder._
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.Breaks._
import scala.collection.JavaConverters._

class PSQL(val originalSQL: String, val dh: DataHub) {

    var SQL: String = originalSQL

    private var cacheName = ""

    private val root: Statement = new Statement("ROOT", SQL)
    private var m: Matcher = _

    //结果集
    val ALL = new mutable.LinkedHashMap[String, Any]()
    var ROWS: Int = 0 //最后一个SELECT返回的结果数量
    var AFFECTED: Int = 0  //最后一个非SELECT语句影响的数据表行数

    //正在解析的所有语句, 控制语句包含ELSE和ELSE_IF
    private val PARSING = new mutable.ArrayStack[Statement]
    //正在执行的控制语句
    private val EXECUTING = new mutable.ArrayStack[Statement]
    //待关闭的控制语句，如IF, FOR, WHILE等，不保存ELSE和ELSE_IF
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
                "USE" -> parseUSE,
                "SAVE" -> parseSAVE,
                "CACHE" -> parseCACHE,
                "TEMP" -> parseTEMP,
                "GET" -> parseGET,
                "PASS" -> parsePASS,
                "PUT" -> parsePUT,
                "PREP" -> parsePREP,
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
        "USE" -> executeUSE,
        "OPEN" -> executeOPEN,
        "SAVE" -> executeSAVE, //
        "CACHE" -> executeCACHE,
        "TEMP" -> executeTEMP,
        "GET" -> executeGET,
        "PASS" -> executePASS,
        "PUT" -> executePUT,
        "PREP" -> executePREP,
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
                parseStatement(sentence.trim.replace("~u003b", ";"))
            }
        }

        PARSING.pop()

        if (PARSING.nonEmpty || TO_BE_CLOSE.nonEmpty) {
            throw new SQLParseException("Control statement hasn't closed: " + PARSING.head.sentence)
        }
    }

    //解析入口
    def parseStatement(sentence: String): Unit = {
        val caption = sentence.trim.takeBefore("""\s""".r).toUpperCase
        if (PARSER.contains(caption)) {
            PARSER(caption)(sentence)
        }
        else if (Set("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE", "ALTER").contains(caption)) {
            PARSING.head.addStatement(new Statement(caption, sentence))
        }
        else {
            throw new SQLParseException("Unrecognized or unsupported sentence: " + sentence)
        }
    }

    private def parseIF(sentence: String): Unit = {
        if ({m = $IF.matcher(sentence); m}.find) {
            val $if: Statement = new Statement("IF", m.group(0), new ConditionGroup(m.group(1)))
            PARSING.head.addStatement($if)
            //只进栈
            PARSING.push($if)
            //待关闭的控制语句
            TO_BE_CLOSE.push($if)
            //继续解析第一条子语句
            parseStatement(sentence.takeAfter(m.group(0)).trim())
        }
        else {
            throw new SQLParseException("Incorrect IF sentence: " + sentence)
        }
    }

    private def parseELSE(sentence: String): Unit = {
        if ({m = $ELSE_IF.matcher(sentence); m}.find) {
            val $elsif: Statement = new Statement("ELSE_IF", m.group(0), new ConditionGroup(m.group(1)))
            if (PARSING.isEmpty || (!(PARSING.head.caption == "IF") && !(PARSING.head.caption == "ELSE_IF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.head.addStatement($elsif)
            PARSING.push($elsif)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else if ({m = $ELSE.matcher(sentence); m}.find) {
            val $else: Statement = new Statement("ELSE")
            if (PARSING.isEmpty || (!(PARSING.head.caption == "IF") && !(PARSING.head.caption == "ELSE_IF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.head.addStatement($else)
            PARSING.push($else)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect ELSE or ELSIF sentence: " + sentence)
        }
    }

    private def parseEND(sentence: String): Unit = {
        if ({m = $END_IF.matcher(sentence); m}.find) {
            //检查IF语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find IF clause: " + m.group)
            }
            else if (!(TO_BE_CLOSE.head.caption == "IF")) {
                throw new SQLParseException(TO_BE_CLOSE.head.caption + " hasn't closed: " + TO_BE_CLOSE.head.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endIf: Statement = new Statement("END_IF")
            //只出栈
            PARSING.pop()
            PARSING.head.addStatement($endIf)
        }
        else if ({m = $END_LOOP.matcher(sentence); m}.find) {
            //检查FOR语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find FOR or WHILE clause: " + m.group)
            }
            else if (!Set("FOR_SELECT", "FOR_IN", "FOR_TO" , "WHILE").contains(TO_BE_CLOSE.head.caption)) {
                throw new SQLParseException(TO_BE_CLOSE.head.caption + " hasn't closed: " + TO_BE_CLOSE.head.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endLoop: Statement = new Statement("END_LOOP")
            //只出栈
            PARSING.pop()
            PARSING.head.addStatement($endLoop)
        }
        else {
            throw new SQLParseException("Incorrect END sentence: " + sentence)
        }
    }

    private def parseFOR(sentence: String): Unit = {
        if ({m = $FOR$SELECT.matcher(sentence); m}.find) {
            val $for$select: Statement = new Statement("FOR_SELECT", m.group(0), new FOR$SELECT(m.group(1).trim, m.group(2).trim))
            PARSING.head.addStatement($for$select)
            //只进栈
            PARSING.push($for$select)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for$select)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else if ({m = $FOR$TO.matcher(sentence); m}.find) {
            val $for$to: Statement = new Statement("FOR_TO", m.group(0), new FOR$TO(m.group(1).trim(), m.group(2).trim(), m.group(3).trim()))
            PARSING.head.addStatement($for$to)

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
            PARSING.head.addStatement($for)
            //只进栈
            PARSING.push($for)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect FOR sentence: " + sentence)
        }
    }

    private def parseWHILE(sentence: String): Unit = {
        if ({m = $WHILE.matcher(sentence); m}.find) {
            val $while: Statement = new Statement("WHILE", m.group(0), new ConditionGroup(m.group(1).trim))
            PARSING.head.addStatement($while)
            //只进栈
            PARSING.push($while)
            //待关闭的控制语句
            TO_BE_CLOSE.push($while)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect WHILE sentence: " + sentence)
        }
    }

    private def parseSET(sentence: String): Unit = {
        if ({m = $SET.matcher(sentence); m}.find) {
            val $set: Statement = new Statement("SET", sentence, new SET(m.group(1).trim, m.group(2).trim))
            PARSING.head.addStatement($set)
        }
        else {
            throw new SQLParseException("Incorrect SET sentence: " + sentence)
        }
    }

    private def parseOPEN(sentence: String): Unit = {
        if ({m = $OPEN.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("OPEN", sentence, new OPEN(m.group(1).trim.split(" ").filter(_ != ""): _*)))
            if (m.group(2).trim == ":") {
                parseStatement(sentence.takeAfter(m.group(2)).trim)
            }
        }
        else {
            throw new SQLParseException("Incorrect OPEN sentence: " + sentence)
        }
    }

    private def parseUSE(sentence: String): Unit = {
        if ({m = $USE.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("USE", sentence, new USE(sentence.substring(4).trim)))
        }
        else {
            throw new SQLParseException("Incorrect USE sentence: " + sentence)
        }
    }

    private def parseSAVE(sentence: String): Unit = {
        //save as
        if ({m = $SAVE$AS.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("SAVE", sentence, new SAVE$AS(m.group(1).trim.split(" ").filter(_ != ""): _*)))
            if (m.group(2).trim == ":") {
                parseStatement(sentence.takeAfter(":").trim)
            }
        }
        else {
            throw new SQLParseException("Incorrect SAVE sentence: " + sentence)
        }
    }

    private def parseCACHE(sentence: String): Unit = {
        if ({m = $CACHE.matcher(sentence); m}.find) {
            val $cache = new Statement("CACHE", sentence.takeBefore("#"), new CACHE(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.head.addStatement($cache)
        }
        else {
            throw new SQLParseException("Incorrect CACHE sentence: " + sentence)
        }
    }

    private def parseTEMP(sentence: String): Unit = {
        if ({m = $TEMP.matcher(sentence); m}.find) {
            val $temp = new Statement("TEMP", sentence.takeBefore("#"), new TEMP(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.head.addStatement($temp)
        }
        else {
            throw new SQLParseException("Incorrect TEMP sentence: " + sentence)
        }
    }

    private def parseGET(sentence: String): Unit = {
        if ({m = $GET.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }

    private def parsePASS(sentence: String): Unit = {
        if ({m = $PASS.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PASS", sentence, new PASS(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PASS sentence: " + sentence)
        }
    }

    private def parsePUT(sentence: String): Unit = {
        if ({m = $PUT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PUT", sentence, new PUT(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PUT sentence: " + sentence)
        }
    }

    private def parsePREP(sentence: String): Unit = {
        if ({m = $PREP.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PREP", sentence, new PREP(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PREP sentence: " + sentence)
        }
    }

    private def parseOUT(sentence: String): Unit = {
        if ({m = $OUT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("OUT", sentence, new OUT(m.group(1).toUpperCase(), m.group(2), sentence.takeAfter("#").trim)))
        }
        else {
            throw new SQLParseException("Incorrect OUT sentence: " + sentence)
        }
    }

    private def parsePRINT(sentence: String): Unit = {
        if ({m = $PRINT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PRINT", sentence, new PRINT(m.group(1), m.group(2).trim)))
        }
        else {
            throw new SQLParseException("Incorrect PRINT sentence: " + sentence)
        }
    }

    private def parseLIST(sentence: String): Unit = {
        if ({m = $LIST.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("LIST", sentence, new LIST(m.group(1))))
        }
        else {
            throw new SQLParseException("Incorrect LIST sentence: " + sentence)
        }
    }

    private def parseSELECT(sentence: String): Unit = {
        PARSING.head.addStatement(new Statement("SELECT", sentence))
    }

    /* EXECUTE */

    private def executeIF(statement: Statement): Unit = {
        if (statement.instance.asInstanceOf[ConditionGroup].evalAll(this, this.dh)) {
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
            if (statement.instance.asInstanceOf[ConditionGroup].evalAll(this, this.dh)) { //替换
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
        this.updateVariableValue(toLoop.variable, toLoop.parseBegin(this))
        EXECUTING.push(statement)
        while (toLoop.hasNext(this)) {
            this.execute(statement.statements)
            this.updateVariableValue(toLoop.variable, this.findVariableValue(toLoop.variable).value.asInstanceOf[Int] + 1)
        }
    }

    private def executeFOR_IN(statement: Statement): Unit = {
        val inMap: ForLoopVariables = statement.instance.asInstanceOf[FOR$IN].computeMap(this)
        FOR_VARIABLES.push(inMap)
        EXECUTING.push(statement)
        while (inMap.hasNext) {
            this.execute(statement.statements)
        }
    }

    private def executeWHILE(statement: Statement): Unit = {
        val whileCondition: ConditionGroup = statement.instance.asInstanceOf[ConditionGroup]
        EXECUTING.push(statement)
        while (whileCondition.evalAll(this, this.dh)) {
            this.execute(statement.statements)
        }
    }

    private def executeEND_LOOP(statement: Statement): Unit = {
        if (Set("FOR_SELECT", "FOR_IN").contains(EXECUTING.head.caption)) {
            FOR_VARIABLES.pop()
        }
        EXECUTING.pop()
    }

    private def executeSET(statement: Statement): Unit = {
        statement.instance.asInstanceOf[SET].assign(this, this.dh)
    }

    private def executeOPEN(statement: Statement): Unit = {
        val $open = statement.instance.asInstanceOf[OPEN]
        $open.sourceType match {
            case "CACHE" => dh.openCache()
            case "TEMP" => dh.openTemp()
            case "DEFAULT" => dh.openDefault()
            case _ => dh.open($open.connectionName.$eval(this), $open.databaseName.$eval(this))
        }
    }

    private def executeUSE(statement: Statement): Unit = {
        val $use = statement.instance.asInstanceOf[USE]
        dh.use($use.databaseName.$eval(this))
    }

    private def executeSAVE(statement: Statement): Unit = {
        val $save = statement.instance.asInstanceOf[SAVE$AS]
        $save.targetType match {
            case "CACHE TABLE" => dh.cache($save.targetName.$eval(this))
            case "TEMP TABLE" => dh.temp($save.targetName.$eval(this))
            case "CACHE" => dh.saveAsCache()
            case "TEMP" => dh.saveAsTemp()
            case "JDBC" =>
                $save.targetName match {
                    case "DEFAULT" => dh.saveAsDefault()
                    case _ => dh.saveAs($save.targetName.$eval(this), $save.databaseName.$eval(this))
                }
            case _ =>
        }
    }

    private def executeCACHE(statement: Statement): Unit = {
        val $cache = statement.instance.asInstanceOf[CACHE]
        dh.get($cache.selectSQL.$place(this)).cache($cache.tableName.$eval(this))
    }

    private def executeTEMP(statement: Statement): Unit = {
        val $temp = statement.instance.asInstanceOf[TEMP]
        dh.get($temp.selectSQL.$place(this)).temp($temp.tableName.$eval(this))
    }

    private def executeGET(statement: Statement): Unit = {
        val $get = statement.instance.asInstanceOf[GET]
        dh.get($get.selectSQL.$place(this))
    }

    private def executePASS(statement: Statement): Unit = {
        val $pass = statement.instance.asInstanceOf[PASS]
        dh.pass($pass.selectSQL.$place(this))
    }

    private def executePUT(statement: Statement): Unit = {
        val $put = statement.instance.asInstanceOf[PUT]
        dh.put($put.nonQuerySQL.$place(this))
    }

    private def executePREP(statement: Statement): Unit = {
        val $prep = statement.instance.asInstanceOf[PREP]
        dh.prep($prep.nonQuerySQL)
    }

    private def executeOUT(statement: Statement): Unit = {
        val $out = statement.instance.asInstanceOf[OUT]
        val outputName = $out.outputName.$eval(this)
        val SQL = $out.SQL.$place(this)

        $out.caption match {
            case "SELECT" =>
                val rows = dh.executeMapList(SQL)
                ROWS = rows.size
                $out.outputType.$eval(this) match {
                    case "SINGLE" =>
                        if (rows.nonEmpty && rows.head.nonEmpty) {
                            for (key <- rows.head.keySet) {
                                ALL.put(outputName, rows.head.get(key))
                            }
                        }
                        else {
                            ALL.put(outputName, "")
                        }
                    case "MAP" =>
                        ALL.put(outputName,
                                if (rows.nonEmpty) {
                                    rows.head
                                }
                                else {
                                    Map[String, Any]()
                                })
                    case "LIST" => ALL.put(outputName, rows)
                    case _ =>
                }
            case _ =>
                //非查询语句仅返回受影响的行数, 输出类型即使设置也无效
                ALL.put(outputName, {
                    AFFECTED = this.dh.executeNonQuery(SQL)
                    AFFECTED
                })
        }
    }

    private def executePRINT(statement: Statement): Unit = {
        val $print = statement.instance.asInstanceOf[PRINT]
        val message = $print.message.$eval(this)
        $print.messageType match {
            case "WARN" => Console.writeWarning(message)
            case "ERROR" => Console.writeException(message)
            case "DEBUG" => Console.writeDebugging(message)
            case "INFO" => Console.writeMessage(message)
            case "NONE" => Console.writeLine(message)
            case seal: String => Console.writeLineWithSeal(seal, message)
            case _ =>
        }
    }

    private def executeLIST(statement: Statement): Unit = {
        val $list = statement.instance.asInstanceOf[LIST]
        dh.show(Try($list.rows.$eval(this).toInt).getOrElse(20))
    }

    private def executeSELECT(statement: Statement): Unit = {
        ALL.put(OUT.LIST, {
            val rows = dh.executeMapList(statement.sentence.$place(this))
            ROWS = rows.size
            rows
        })

    }

    private def execute(statements: ArrayBuffer[Statement]): Unit = {

        for (statement <- statements) {
            if (EXECUTOR.contains(statement.caption)) {
                EXECUTOR(statement.caption)(statement)
            }
            else {
                ALL.put(OUT.AFFECTED, {
                    AFFECTED = dh.executeNonQuery(statement.sentence)
                    AFFECTED
                })
            }
        }
    }

    //程序变量相关
    //root块存储程序级全局变量
    //其他局部变量在子块中
    def updateVariableValue(field: String, value: Any): Unit = {
        val symbol = field.take(1)
        val name = field.takeAfter(0).$trim("(", ")").toUpperCase()

        if (symbol == "$") {
            //局部变量
            var found = false
            breakable {
                for (i <- FOR_VARIABLES.indices) {
                    if (FOR_VARIABLES(i).contains(name)) {
                        FOR_VARIABLES(i).set(name, value)
                        found = true
                        break
                    }
                }
            }

            if (!found) {
                breakable {
                    for (i <- EXECUTING.indices) {
                        if (EXECUTING(i).containsVariable(name)) {
                            EXECUTING(i).setVariable(name, value)
                            found = true
                            break
                        }
                    }
                }
            }

            if (!found) {
                EXECUTING.head.setVariable(name, value)
            }

        }
        else if (symbol == "@") {
            //全局变量
            GlobalVariable.set(name, value, dh.userId, dh.roleName)
        }
    }

    //变量名称存储时均为大写, 即在执行过程中不区分大小写
    def findVariableValue(field: String): DataCell = {

        val symbol = field.take(1)
        val name = field.takeAfter(0).$trim("(", ")").toUpperCase()

        var cell = new DataCell(null)

        if (symbol == "$") {
            breakable {
                for (i <- FOR_VARIABLES.indices) {
                    if (FOR_VARIABLES(i).contains(name)) {
                        cell = FOR_VARIABLES(i).get(name)
                        break
                    }
                }
            }

            breakable {
                for (i <- EXECUTING.indices) {
                    if (EXECUTING(i).containsVariable(name)) {
                        cell = EXECUTING(i).getVariable(name)
                        break
                    }
                }
            }
        }
        else if (symbol == "@") {
            //全局变量
            GlobalVariable.get(name, this)
        }

        cell
    }

    //传递参数和数据, Spring Boot的httpRequest参数
    def $with(args: Any): PSQL = {
        this.SQL = this.SQL.replaceArguments(args match {
            case queries : java.util.Map[String, Array[String]] => queries.asScala.map(kv => (kv._1, kv._2(0))).toMap
            case args: Map[String, String] => args
            case queryString: String => queryString.toHashMap()
            case _ => Map[String, String]()
        })
        this
    }

    //设置单个变量的值
    def set(globalVariableName: String, value: Any): PSQL = {
        root.setVariable(globalVariableName, value)
        this
    }

    def run(): PSQL = {
        this.parseAll()
        //Console.writeLine("# FROM DATASOURCE # " + this.SQL)
        EXECUTING.push(root)
        this.execute(root.statements)
        dh.clear()

        this
    }

    def $return(resultType: String = OUT.LIST): Option[Any] = {

        dh.close()

        val outType = resultType.toLowerCase()
        if (outType == OUT.ALL) {
            Some(ALL)
        }
        else  {
            ALL.get(outType)
        }
    }

    def show(): Unit = {
        val sentences = SQL.split(";")
        for (i <- sentences.indices) {
            Console.writeLine(i, ": ", sentences(i))
        }
        Console.writeLine("------------------------------------------------------------")
        this.root.show(0)
    }
}