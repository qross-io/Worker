
private val groups = immutable.HashMap[String, Int](
    DBType.SQLite -> DBGroup.DBS,
    DBType.MySQL -> DBGroup.RDBMS,
    DBType.SQLServer -> DBGroup.RDBMS,
    DBType.Hive -> DBGroup.DWS,
    DBType.Spark -> DBGroup.DWS,
    DBType.Impala -> DBGroup.DWS,
    DBType.Oracle -> DBGroup.RDBMS,
    //case "h2" => DBGroup.DBS
    DBType.None -> DBGroup.BASE
)

def group(connectionName: String, connectionString: String = ""): Int = {
    var dbGroup = DBGroup.BASE
    breakable {
        for (name <- groups.keySet) {
            if (connectionName.toLowerCase().startsWith(name) || connectionString.toLowerCase().contains(name)) {
                dbGroup = groups(name)
                break
            }
        }
    }
    dbGroup
}

if (dbType == DBType.Oracle && userName == "" && password == "") {
    password = connectionString.substring(connectionString.lastIndexOf(":") + 1)
    connectionString = connectionString.substring(0, connectionString.lastIndexOf(":"))
    userName = connectionString.substring(connectionString.lastIndexOf(":") + 1)
    connectionString = connectionString.substring(0, connectionString.lastIndexOf(":"))
}