package io.qross.sql

import io.qross.core.DataHub
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._
import io.qross.ext.PlaceHolder._

class SET(var variable: String, expression: String) {

    //variable = variable.removeVariableModifier()

    if (variable.contains(",")) {
        if (!$SELECT.matcher(expression).find) {
            throw new SQLParseException("Multiple variables definition only support SELECT sentence. " + expression)
        }
    }

    def assign(PSQL: PSQL, dh: DataHub): Unit = {
        //1. SELECT查询  - 以SELECT开头 - 需要解析$开头的变量和函数
        //2. 非SELECT查询 - 以INSERT,UPDATE,DELETE开头 - 需要解析$开头的变量和函数
        //3. 字符串赋值或连接 - 用双引号包含，变量和内部函数需要加$前缀 - 需要解析$开头的变量和函数
        //4. 执行函数 - 以函数名开头，不需要加$前缀 - 直接执行函数
        //5. 变量间直接赋值 - 是变量格式(有$前缀)且是存在于变量列表中的变量 - 直接赋值
        //6. 数学表达式 - 其他 - 解析函数和变量然后求值，出错则抛出异常
        var expression = this.expression
        if ($SELECT.matcher(expression).find) { //SELECT
            expression = expression.$place(PSQL)

            val names = variable.split(",")
            val values = dh.executeDataRow(expression).getValues
            for (i <- names.indices) {
                PSQL.updateVariableValue(names(i).trim, if (i < values.length) values(i) else "null")
            }
        }
        else if ($NON_QUERY.matcher(expression).find) {
            //INSERT + UPDATE + DELETE
            expression = expression.$place(PSQL)
            PSQL.updateVariableValue(variable, dh.executeNonQuery(expression))
        }
        else {
            //其他，以标准表达式处理
            PSQL.updateVariableValue(variable, expression.evalExpression(PSQL).value)
        }
    }
}
