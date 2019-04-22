package io.qross.util

import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}


object Script {
    
    def eval(expression: String): DataCell = {
        val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
        try {
            new DataCell(jse.eval(expression))
        }
        catch {
            case e: ScriptException =>
                e.printStackTrace()
                null
        }
        
        //The code below doesn't work.
        //import scala.tools.nsc._
        //val interpreter = new Interpreter(new Settings())
        //interpreter.interpret("import java.text.{SimpleDateFormat => sdf}")
        //interpreter.bind("date", "java.util.Date", new java.util.Date());
        //interpreter.eval[String]("""new sdf("yyyy-MM-dd").format(date)""") get
        //    interpreter.close()
    }
}
