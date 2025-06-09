package com.imprint.types;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import lombok.Getter;

/**
 * Type codes for Imprint values.
 */
public enum TypeCode {
    NULL(0x0, TypeHandler.NULL),
    BOOL(0x1, TypeHandler.BOOL),
    INT32(0x2, TypeHandler.INT32),
    INT64(0x3, TypeHandler.INT64),
    FLOAT32(0x4, TypeHandler.FLOAT32),
    FLOAT64(0x5, TypeHandler.FLOAT64),
    BYTES(0x6, TypeHandler.BYTES),
    STRING(0x7, TypeHandler.STRING),
    ARRAY(0x8, TypeHandler.ARRAY),
    MAP(0x9, TypeHandler.MAP),
    ROW(0xA, null);   // TODO: implement (basically a placeholder for user-defined type)

    @Getter
    private final byte code;
    private final TypeHandler handler;

    private static final TypeCode[] LOOKUP = new TypeCode[11];

    static {
        for (var type : values()) {
            LOOKUP[type.code] = type;
        }
    }

    TypeCode(int code, TypeHandler handler) {
        this.code = (byte) code;
        this.handler = handler;
    }

    public TypeHandler getHandler() {
        if (handler == null) {
            throw new UnsupportedOperationException("Handler not implemented for " + this);
        }
        return handler;
    }

    public static TypeCode fromByte(byte code) throws ImprintException {
        if (code >= 0 && code < LOOKUP.length) {
            var type = LOOKUP[code];
            if (type != null) return type;
        }
        throw new ImprintException(ErrorType.INVALID_TYPE_CODE,
                "Unknown type code: 0x" + Integer.toHexString(code & 0xFF));
    }
}