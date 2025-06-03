package com.imprint.error;

/**
 * Types of errors that can occur in Imprint operations.
 */
public enum ErrorType {
    INVALID_MAGIC,
    UNSUPPORTED_VERSION,
    BUFFER_UNDERFLOW,
    FIELD_NOT_FOUND,
    SCHEMA_ERROR,
    INVALID_UTF8_STRING,
    MALFORMED_VARINT,
    TYPE_MISMATCH,
    INVALID_TYPE_CODE,
    SERIALIZATION_ERROR,
    DESERIALIZATION_ERROR,
    INTERNAL_ERROR
}
