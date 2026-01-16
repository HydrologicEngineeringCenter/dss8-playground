package mil.army.usace.hec.sqldss.api;

public class ApiException extends Exception {
    public ApiException() { super(); }
    public ApiException(String message) { super(message); }
    public ApiException(String message, Throwable cause) { super(message, cause); }
    public ApiException(Throwable cause) { super(cause); }
}
