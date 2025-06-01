package com.imprint.error;

import lombok.Getter;

/**
 * Exception thrown by Imprint operations.
 */
@Getter
public class ImprintException extends Exception {
    private final ErrorType errorType;
    
    public ImprintException(ErrorType errorType, String message) {
        super(message);
        this.errorType = errorType;
    }
    
    public ImprintException(ErrorType errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    @Override
    public String toString() {
        return String.format("ImprintException{type=%s, message='%s'}", errorType, getMessage());
    }
}
