package io.qross.sql;

import io.qross.util.DataRow;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class ForInLoop {

    public Statement statement;

    public String forCollection;
    public String delimiter; //default ,
    public List<String> fields = new ArrayList<>();
    public List<String> separators = new ArrayList<>();

    //ForSelectLoop(String variables, String selectSQL);
    public ForInLoop(Statement statement, String forItems, String forCollection, String delimiter) {

        this.statement = statement;

        this.forCollection = forCollection;
        this.delimiter = Statement.removeQuotes(delimiter);

        //new code, both supports ${var} and $var
        Matcher m = Statement.$VARIABLE.matcher(forItems);
        while (m.find()) {
            String var = m.group();
            int index = forItems.indexOf(var);
            if (index > 0) {
                separators.add(forItems.substring(0, index));
            }
            else {
                separators.add("");
            }
            fields.add(Statement.removeVariableModifier(var));

            forItems = forItems.substring(index + var.length());
        }

        if (!forItems.isEmpty()) {
            this.separators.add(forItems);
        } else {
            this.separators.add("");
        }
    }

    public ForLoopVariables computeMap() {

        ForLoopVariables variablesMaps = new ForLoopVariables();

        String[] collection = this.statement.parseStandardSentence(this.forCollection).split(this.delimiter, -1);

        for (String line : collection) {
            DataRow row = new DataRow();
            for (int j = 0; j < this.fields.size(); j++) {
                String field = this.fields.get(j);
                String prefix = this.separators.get(j);
                String suffix = this.separators.get(j+1);
                line = line.substring(prefix.length());
                if (suffix.isEmpty()) {
                    row.set(field, line);
                    //line = ""; //set empty
                    break; //continue next line
                }
                else {
                    row.set(field, line.substring(0, line.indexOf(suffix)));
                    line = line.substring(line.indexOf(suffix));
                }
            }
            variablesMaps.addRow(row);
        }

        return variablesMaps;
    }
}
