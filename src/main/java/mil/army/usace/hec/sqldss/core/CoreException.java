package mil.army.usace.hec.sqldss.core;

public class CoreException extends Exception {
    public CoreException() { super(); }
    public CoreException(String message) { super(message); }
    public CoreException(String message, Throwable cause) { super(message, cause); }
    public CoreException(Throwable cause) { super(cause); }
}
