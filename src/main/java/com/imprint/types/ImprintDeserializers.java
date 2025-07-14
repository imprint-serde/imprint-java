package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.ImprintBuffer;
import com.imprint.util.VarInt;
import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Static primitive deserialization methods for all Imprint types.
 * Returns native Java objects instead of Value wrappers for better performance.
 */
@UtilityClass
public final class ImprintDeserializers {
    
    // Primitive deserializers
    public static Object deserializePrimitive(ImprintBuffer buffer, TypeCode typeCode) throws ImprintException {
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
                int byteLength = VarInt.decode(buffer).getValue();
                if (buffer.remaining() < byteLength) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, 
                        "Not enough bytes for bytes value data after VarInt. Needed: " + 
                        byteLength + ", available: " + buffer.remaining());
                }
                byte[] bytes = new byte[byteLength];
                buffer.get(bytes);
                return bytes;
            case STRING:
                int strLength = VarInt.decode(buffer).getValue();
                if (buffer.remaining() < strLength) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                        "Not enough bytes for string value data after VarInt. Needed: " +
                        strLength + ", available: " + buffer.remaining());
                }
                byte[] stringBytes = new byte[strLength];
                buffer.get(stringBytes);
                return new String(stringBytes, StandardCharsets.UTF_8);
            case DATE:
                if (buffer.remaining() < 4) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for date");
                }
                int daysSinceEpoch = buffer.getInt();
                return LocalDate.ofEpochDay(daysSinceEpoch);
            case TIME:
                if (buffer.remaining() < 4) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for time");
                }
                int millisSinceMidnight = buffer.getInt();
                return LocalTime.ofNanoOfDay(millisSinceMidnight * 1_000_000L);
            case UUID:
                if (buffer.remaining() < 16) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for UUID");
                }
                long mostSignificantBits = buffer.getLong();
                long leastSignificantBits = buffer.getLong();
                return new UUID(mostSignificantBits, leastSignificantBits);
            case DECIMAL:
                int scale = VarInt.decode(buffer).getValue();
                int unscaledLength = VarInt.decode(buffer).getValue();
                if (buffer.remaining() < unscaledLength) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                        "Not enough bytes for decimal unscaled value. Needed: " +
                        unscaledLength + ", available: " + buffer.remaining());
                }
                byte[] unscaledBytes = new byte[unscaledLength];
                buffer.get(unscaledBytes);
                var unscaledValue = new BigInteger(unscaledBytes);
                return new BigDecimal(unscaledValue, scale);
            case TIMESTAMP:
                if (buffer.remaining() < 4) {
                    throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for timestamp");
                }
                int millisSinceEpoch = buffer.getInt();
                return Instant.ofEpochMilli(millisSinceEpoch);
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Cannot deserialize " + typeCode + " as primitive");
        }
    }
}