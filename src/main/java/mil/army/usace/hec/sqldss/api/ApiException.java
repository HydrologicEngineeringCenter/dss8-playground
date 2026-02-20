package mil.army.usace.hec.sqldss.api;

/**
 * Exception class for API layers
 */
public class ApiException extends Exception {
    /**
     * Default contsturctor
     */
    public ApiException() { super(); }

    /**
     * Constructor with message
     * @param message The message
     */
    public ApiException(String message) { super(message); }

    /**
     * Constructor with message and cause
     * @param message The message
     * @param cause The cause
     */
    public ApiException(String message, Throwable cause) { super(message, cause); }

    /**
     * Constructor with cause
     * @param cause the cause
     */
    public ApiException(Throwable cause) { super(cause); }
}
