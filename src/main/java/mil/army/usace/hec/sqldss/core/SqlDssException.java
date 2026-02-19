package mil.army.usace.hec.sqldss.core;

public class SqlDssException extends Exception {
    public SqlDssException() { super(); }
    public SqlDssException(String message) { super(message); }
    public SqlDssException(String message, Throwable cause) { super(message, cause); }
    public SqlDssException(Throwable cause) { super(cause); }
}
