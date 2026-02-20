package mil.army.usace.hec.sqldss.core;

/**
 * Exception class for EncodedDateTime operations
 */
public class EncodedDateTimeException extends Exception{
    /**
     * Default constructor
     */
    public EncodedDateTimeException() { super(); }

    /**
     * Constructor with message
     * @param message The message
     */
    public EncodedDateTimeException(String message) { super(message); }

    /**
     * Constructor with message and cause
     * @param message The message
     * @param cause The cause
     */
    public EncodedDateTimeException(String message, Throwable cause) { super(message, cause); }

    /**
     * Constructor with cause
     * @param cause The cause
     */
    public EncodedDateTimeException(Throwable cause) { super(cause); }
}
