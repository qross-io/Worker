


while(cursor < sentences.length) {
    val sentence = sentences(cursor).trim
    if (sentence.nonEmpty) {
        parseStatement(sentence.replace("~u003b", ";"))
    }
    cursor += 1
}

PSQL parseStatement
//old code - to be remove
if ({m = $IF.matcher(SQL); m}.find) {
    val $if: Statement = this.createStatement("IF", m.group(0), m.group(1))
    $if.closed = false
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
        TO_BE_CLOSE.last.closed = true
        TO_BE_CLOSE.pop()
    }
    val $endIf: Statement = this.createStatement("END_IF", m.group(0))
    //只出栈
    PARSING.pop
    PARSING.last.addStatement($endIf)
}
else if ({m = $FOR$SELECT.matcher(SQL); m}.find) {
    val $for$select: Statement = this.createStatement("FOR_SELECT", m.group(0), m.group(1).trim, m.group(2).trim)
    $for$select.closed = false
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
    $for$to.closed = false
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
    $for.closed = false
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
    $while.closed = false
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
        TO_BE_CLOSE.last.closed = true
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