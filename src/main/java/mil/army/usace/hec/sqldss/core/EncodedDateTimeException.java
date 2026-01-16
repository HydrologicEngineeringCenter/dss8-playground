package mil.army.usace.hec.sqldss.core;

public class EncodedDateTimeException extends Exception{
    public EncodedDateTimeException() { super(); }
    public EncodedDateTimeException(String message) { super(message); }
    public EncodedDateTimeException(String message, Throwable cause) { super(message, cause); }
    public EncodedDateTimeException(Throwable cause) { super(cause); }
}
