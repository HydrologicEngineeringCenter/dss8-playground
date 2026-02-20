package mil.army.usace.hec.sqldss.core;

/**
 * Exception for core SqlDss operations
 */
public class SqlDssException extends Exception {
    /**
     * Default constructor
     */
    public SqlDssException() { super(); }

    /**
     * Constructor with message
     * @param message The message
     */
    public SqlDssException(String message) { super(message); }

    /**
     * Constructor with message and casue
     * @param message The message
     * @param cause The cause
     */
    public SqlDssException(String message, Throwable cause) { super(message, cause); }

    /**
     * Constructor with cause
     * @param cause The cause
     */
    public SqlDssException(Throwable cause) { super(cause); }
}
