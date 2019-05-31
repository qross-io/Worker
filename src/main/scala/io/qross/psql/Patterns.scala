package io.qross.psql

import java.util.regex.Pattern

object Patterns {

    val $SET: Pattern = Pattern.compile("""^SET\s+([@\$].+?):=(.+)$""", Pattern.CASE_INSENSITIVE) //"""^SET\s+(\$\{?[a-z_][a-z0-9_]*}?(\s*,\s*\$\{?[a-z_][a-z0-9_]*}?)*\s*):="""
    val $NAME: Pattern = Pattern.compile("^([a-z][a-z0-9_#]*)?\\s*:", Pattern.CASE_INSENSITIVE)
    val $TRY: Pattern = Pattern.compile("""^TRY\s+(\S+?)\s+?#""", Pattern.CASE_INSENSITIVE)
    val $IF: Pattern = Pattern.compile("^IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE)
    val $ELSE_IF: Pattern = Pattern.compile("^ELSE? ?IF\\s+(.+?)\\s+THEN", Pattern.CASE_INSENSITIVE)
    val $ELSE: Pattern = Pattern.compile("^ELSE", Pattern.CASE_INSENSITIVE)
    val $END_IF: Pattern = Pattern.compile("""^END\s*IF""", Pattern.CASE_INSENSITIVE)

    val $FOR$SELECT: Pattern = Pattern.compile( """^FOR\s+(.+?)\s+IN\s+(\(SELECT\s+.+\))\s+LOOP""", Pattern.CASE_INSENSITIVE)
    val $FOR$TO: Pattern = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)\\s+TO\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE)
    val $FOR$IN: Pattern = Pattern.compile("^FOR\\s+(.+?)\\s+IN\\s+(.+?)(\\s+DELIMITED\\s+BY\\s+(.+))?\\s+LOOP", Pattern.CASE_INSENSITIVE)
    val $WHILE: Pattern = Pattern.compile("^WHILE\\s+(.+)\\s+LOOP", Pattern.CASE_INSENSITIVE)
    val $END_LOOP: Pattern = Pattern.compile("^END\\s*LOOP", Pattern.CASE_INSENSITIVE)
    val $SPACE: Pattern = Pattern.compile("""\s""")

    val CONDITION: String = "#[condition:"
    val N: String = "]"
    val $SELECT$: Pattern = Pattern.compile("""\(\s*SELECT\s""", Pattern.CASE_INSENSITIVE)  //(SELECT...)
    val SELECT$: Pattern = Pattern.compile("#\\[select:(\\d+?)]")
    val $EXISTS: Pattern = Pattern.compile("EXISTS\\s*(\\([^)]+\\))", Pattern.CASE_INSENSITIVE)
    val EXISTS$: Pattern = Pattern.compile("#\\[exists:(\\d+?)]")
    val $IN: Pattern = Pattern.compile("\\sIN\\s*(\\([^)]+\\))", Pattern.CASE_INSENSITIVE)
    val IN$: Pattern = Pattern.compile("#\\[in:(\\d+?)]")
    val $BRACKET: Pattern = Pattern.compile("\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE)
    val $AND: Pattern = Pattern.compile("(^|\\sOR\\s)((.+?)\\s+AND\\s+(.+?))($|\\sAND|\\sOR)", Pattern.CASE_INSENSITIVE)
    val $_OR: Pattern = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE)
    val $OR: Pattern = Pattern.compile("(^)((.+?)\\s+OR\\s+(.+?))(\\sOR|$)", Pattern.CASE_INSENSITIVE)
    val CONDITION$: Pattern = Pattern.compile("#\\[condition:(\\d+)]", Pattern.CASE_INSENSITIVE)

    val $FUNCTION: Pattern = Pattern.compile("\\$(" + Function.NAMES.mkString("|") + ")\\s*\\(([^()]*)\\)", Pattern.CASE_INSENSITIVE)
    val $STRING: Pattern = Pattern.compile("""#\{s(\d+)}""", Pattern.CASE_INSENSITIVE)

    val $SELECT: Pattern = Pattern.compile("^SELECT\\s", Pattern.CASE_INSENSITIVE)
    val $NON_QUERY: Pattern = Pattern.compile("^(INSERT|UPDATE|DELETE)\\s", Pattern.CASE_INSENSITIVE)

    //val $FUNCTION: Pattern = Pattern.compile("\\$\\{?(" + Function.NAMES.mkString("|") + ")\\s*\\(", Pattern.CASE_INSENSITIVE)
    //val $VARIABLE: Pattern = Pattern.compile("""\$\(?([a-z_][a-z0-9_]*))?""", Pattern.CASE_INSENSITIVE)

    //v0.5.9
    //"""^OPEN\s+((\S+\s+)*?)(\S+)(\s+USE\s+(\S+))?\s*(:|$)"""
    val $OPEN: Pattern = Pattern.compile("""^OPEN\s+.+?(:\s+|$)""", Pattern.CASE_INSENSITIVE)
    val $USE: Pattern = Pattern.compile("""^USE\s+""")
    val $SAVE_AS: Pattern = Pattern.compile("""^SAVE\s*AS\s+((\S+\s+)*?)(\S+)(\s+USE\s+(\S+))?\s*:?$""", Pattern.CASE_INSENSITIVE)
    val $CACHE: Pattern = Pattern.compile("""^CACHE\s+(\S+)\s*#""", Pattern.CASE_INSENSITIVE)
    val $TEMP: Pattern = Pattern.compile("""^TEMP\s+(\S+)\s*#""", Pattern.CASE_INSENSITIVE)
    val $GET: Pattern = Pattern.compile("""^GET\s*#""", Pattern.CASE_INSENSITIVE)
    val $PASS: Pattern = Pattern.compile("""^PASS\s*#""", Pattern.CASE_INSENSITIVE)
    val $PUT: Pattern = Pattern.compile("""^PUT\s*#""", Pattern.CASE_INSENSITIVE)
    val $OUT: Pattern = Pattern.compile("""^OUT\s+(SINGLE|MAP|LIST|AFFECTED)?\s+(\S+)\s*#""", Pattern.CASE_INSENSITIVE)
    val $PRINT: Pattern = Pattern.compile("""^PRINT\s+?([a-z]+\s+)?(.+)$""", Pattern.CASE_INSENSITIVE)
    val $LIST: Pattern = Pattern.compile("""^LIST\s+(\d+)""", Pattern.CASE_INSENSITIVE)
}
