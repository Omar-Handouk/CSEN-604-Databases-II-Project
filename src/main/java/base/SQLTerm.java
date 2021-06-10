package base;

import java.util.Hashtable;

public class SQLTerm {
    public static final String[] OPERATORS = {">", ">=", "<", "<=", "=", "!="};

    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm() {}

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }

    public String get_strTableName() {
        return _strTableName;
    }

    public String get_strColumnName() {
        return _strColumnName;
    }

    public String get_strOperator() {
        return _strOperator;
    }

    public Object get_objValue() {
        return _objValue;
    }
}
