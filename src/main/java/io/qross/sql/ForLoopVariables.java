package io.qross.sql;

import io.qross.util.DataCell;
import io.qross.util.DataRow;

import java.util.ArrayList;
import java.util.List;

public class ForLoopVariables {

    //循环列表项及值
    private List<DataRow> variables = new ArrayList<>();
    private int cursor = -1;

    public void addRow(DataRow row) {
        this.variables.add(row);
    }

    public boolean hasNext() {
        this.cursor++;
        return this.cursor < variables.size();
    }

    public boolean contains(String field) {
        if (this.variables.size() > 0) {
            return this.variables.get(0).contains(field);
        }
        else {
            return false;
        }
    }

    public DataCell get(String field) {
        return this.variables.get(this.cursor).getCell(field);
    }

    public void set(String field, Object value) {
        this.variables.get(this.cursor).set(field, value);
    }
}
