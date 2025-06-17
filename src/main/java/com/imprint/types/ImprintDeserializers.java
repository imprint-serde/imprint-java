package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;

/**
 * Static primitive deserialization methods for all Imprint types.
 * Returns native Java objects instead of Value wrappers for better performance.
 */
@UtilityClass
public final class ImprintDeserializers {
    
    // Primitive deserializers
    public static Object deserializePrimitive(ByteBuffer buffer, TypeCode typeCode) throws ImprintException {
        switch (typeCode) {
            case NULL:
                return null;
            case BOOL:
                if (buffer.remaining() < 1) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for bool");
                }
                byte boolByte = buffer.get();
                if (boolByte == 0) return false;
                if (boolByte == 1) return true;
                throw new ImprintException(ErrorType.SCHEMA_ERROR, "Invalid boolean value: " + boolByte);
            case INT32:
                if (buffer.remaining() < 4) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int32");
                }
                return buffer.getInt();
            case INT64:
                if (buffer.remaining() < 8) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int64");
                }
                return buffer.getLong();
            case FLOAT32:
                if (buffer.remaining() < 4) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float32");
                }
                return buffer.getFloat();
            case FLOAT64:
                if (buffer.remaining() < 8) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float64");
                }
                return buffer.getDouble();
            case BYTES:
                VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
                int length = lengthResult.getValue();
                if (buffer.remaining() < length) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, 
                        "Not enough bytes for bytes value data after VarInt. Needed: " + 
                        length + ", available: " + buffer.remaining());
                }
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return bytes;
            case STRING:
                VarInt.DecodeResult strLengthResult = VarInt.decode(buffer);
                int strLength = strLengthResult.getValue();
                if (buffer.remaining() < strLength) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, 
                        "Not enough bytes for string value data after VarInt. Needed: " + 
                        strLength + ", available: " + buffer.remaining());
                }
                byte[] stringBytes = new byte[strLength];
                buffer.get(stringBytes);
                return new String(stringBytes, java.nio.charset.StandardCharsets.UTF_8);
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Cannot deserialize " + typeCode + " as primitive");
        }
    }
}