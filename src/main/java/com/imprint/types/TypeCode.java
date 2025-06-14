package com.imprint.types;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import lombok.Getter;

/**
 * Type codes for Imprint values.
 */
public enum TypeCode {
    NULL(0x0),
    BOOL(0x1),
    INT32(0x2),
    INT64(0x3),
    FLOAT32(0x4),
    FLOAT64(0x5),
    BYTES(0x6),
    STRING(0x7),
    ARRAY(0x8),
    MAP(0x9),
    ROW(0xA);   // TODO: implement (basically a placeholder for user-defined type)

    @Getter
    private final byte code;

    private static final TypeCode[] LOOKUP = new TypeCode[11];

    static {
        for (var type : values()) {
            LOOKUP[type.code] = type;
        }
    }

    TypeCode(int code) {
        this.code = (byte) code;
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