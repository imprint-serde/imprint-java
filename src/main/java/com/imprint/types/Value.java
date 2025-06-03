package com.imprint.types;

import com.imprint.core.ImprintRecord;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A value that can be stored in an Imprint record.
 */
public abstract class Value {
    
    public abstract TypeCode getTypeCode();
    public abstract boolean equals(Object obj);
    public abstract int hashCode();
    public abstract String toString();
    
    // Factory methods
    public static Value nullValue() {
        return NullValue.INSTANCE;
    }
    
    public static Value fromBoolean(boolean value) {
        return new BoolValue(value);
    }
    
    public static Value fromInt32(int value) {
        return new Int32Value(value);
    }
    
    public static Value fromInt64(long value) {
        return new Int64Value(value);
    }
    
    public static Value fromFloat32(float value) {
        return new Float32Value(value);
    }
    
    public static Value fromFloat64(double value) {
        return new Float64Value(value);
    }
    
    public static Value fromBytes(byte[] value) {
        return new BytesValue(value);
    }
    
    public static Value fromBytesBuffer(ByteBuffer value) {
        return new BytesBufferValue(value);
    }
    
    public static Value fromString(String value) {
        return new StringValue(value);
    }
    
    public static Value fromStringBuffer(ByteBuffer value) {
        return new StringBufferValue(value);
    }
    
    public static Value fromArray(List<Value> value) {
        return new ArrayValue(value);
    }
    
    public static Value fromMap(Map<MapKey, Value> value) {
        return new MapValue(value);
    }
    
    public static Value fromRow(ImprintRecord value) {
        return new RowValue(value);
    }
    
    // Null Value
    @EqualsAndHashCode(callSuper = false)
    public static class NullValue extends Value {
        public static final NullValue INSTANCE = new NullValue();
        
        private NullValue() {}
        
        @Override
        public TypeCode getTypeCode() { return TypeCode.NULL; }
        
        @Override
        public String toString() {
            return "null";
        }
    }
    
    // Boolean Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class BoolValue extends Value {
        private final boolean value;
        
        public BoolValue(boolean value) {
            this.value = value;
        }
        
        public boolean getValue() { return value; }
        
        @Override
        public TypeCode getTypeCode() { return TypeCode.BOOL; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    // Int32 Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Int32Value extends Value {
        private final int value;
        
        public Int32Value(int value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.INT32; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    // Int64 Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Int64Value extends Value {
        private final long value;
        
        public Int64Value(long value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.INT64; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    // Float32 Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Float32Value extends Value {
        private final float value;
        
        public Float32Value(float value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.FLOAT32; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    // Float64 Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Float64Value extends Value {
        private final double value;
        
        public Float64Value(double value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.FLOAT64; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    // Bytes Value (array-based)
    public static class BytesValue extends Value {
        private final byte[] value;
        
        public BytesValue(byte[] value) {
            this.value = value.clone(); // defensive copy
        }
        
        public byte[] getValue() { 
            return value.clone(); // defensive copy
        }
        
        @Override
        public TypeCode getTypeCode() { return TypeCode.BYTES; }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (obj instanceof BytesValue) {
                BytesValue that = (BytesValue) obj;
                return Arrays.equals(value, that.value);
            }
            if (obj instanceof BytesBufferValue) {
                BytesBufferValue that = (BytesBufferValue) obj;
                return Arrays.equals(value, that.getValue());
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
        
        @Override
        public String toString() {
            return "bytes[" + value.length + "]";
        }
    }
    
    // Bytes Value (ByteBuffer-based, zero-copy)
    public static class BytesBufferValue extends Value {
        private final ByteBuffer value;
        
        public BytesBufferValue(ByteBuffer value) {
            this.value = value.asReadOnlyBuffer(); // zero-copy read-only view
        }
        
        public byte[] getValue() { 
            // Fallback to array when needed
            byte[] array = new byte[value.remaining()];
            value.duplicate().get(array);
            return array;
        }
        
        public ByteBuffer getBuffer() {
            return value.duplicate(); // zero-copy view
        }
        
        @Override
        public TypeCode getTypeCode() { return TypeCode.BYTES; }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (obj instanceof BytesBufferValue) {
                BytesBufferValue that = (BytesBufferValue) obj;
                return value.equals(that.value);
            }
            if (obj instanceof BytesValue) {
                BytesValue that = (BytesValue) obj;
                return Arrays.equals(getValue(), that.getValue());
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return value.hashCode();
        }
        
        @Override
        public String toString() {
            return "bytes[" + value.remaining() + "]";
        }
    }
    
    // String Value (String-based)
    public static class StringValue extends Value {
        @Getter
        private final String value;
        private volatile byte[] cachedUtf8Bytes; // Cache UTF-8 encoding
        
        public StringValue(String value) {
            this.value = Objects.requireNonNull(value, "String cannot be null");
        }

        public byte[] getUtf8Bytes() {
            var cached = cachedUtf8Bytes;
            if (cached == null) {
                // Multiple threads may compute this - that's OK since it's idempotent
                cached = value.getBytes(StandardCharsets.UTF_8);
                cachedUtf8Bytes = cached;
            }
            return cached; // Return our computed value, not re-read from volatile field
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.STRING; }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (obj instanceof StringValue) {
                StringValue that = (StringValue) obj;
                return value.equals(that.value);
            }
            if (obj instanceof StringBufferValue) {
                StringBufferValue that = (StringBufferValue) obj;
                return value.equals(that.getValue());
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return value.hashCode();
        }
        
        @Override
        public String toString() {
            return "\"" + value + "\"";
        }
    }
    
    // String Value (ByteBuffer-based)
    public static class StringBufferValue extends Value {
        private final ByteBuffer value;
        private volatile String cachedString; // lazy decode

        public StringBufferValue(ByteBuffer value) {
            this.value = value.asReadOnlyBuffer(); // zero-copy read-only view
        }

        public String getValue() {
            var result = cachedString;
            if (result == null) {
                result = decodeUtf8();
                cachedString = result;
            }
            return result;
        }

        private String decodeUtf8() {
            // Zero-copy path: use underlying array directly with correct offset
            if (value.hasArray()) {
                byte[] array = value.array();
                int offset = value.arrayOffset() + value.position();
                int length = value.remaining();
                return new String(array, offset, length, StandardCharsets.UTF_8);
            }
            // Fallback path for direct ByteBuffers (rare case)
            byte[] tempBuffer = new byte[value.remaining()];
            value.duplicate().get(tempBuffer);
            return new String(tempBuffer, StandardCharsets.UTF_8);
        }

        public ByteBuffer getBuffer() {
            return value.duplicate(); // zero-copy view
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.STRING; }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (obj instanceof StringBufferValue) {
                StringBufferValue that = (StringBufferValue) obj;
                return value.equals(that.value);
            }
            if (obj instanceof StringValue) {
                StringValue that = (StringValue) obj;
                return getValue().equals(that.getValue());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return getValue().hashCode(); // Use string hash for consistency
        }

        @Override
        public String toString() {
            return "\"" + getValue() + "\"";
        }
    }
    
    // Array Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class ArrayValue extends Value {
        private final List<Value> value;
        
        public ArrayValue(List<Value> value) {
            this.value = List.copyOf(Objects.requireNonNull(value, "Array cannot be null"));
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.ARRAY; }
        
        @Override
        public String toString() {
            return value.toString();
        }
    }
    
    // Map Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class MapValue extends Value {
        private final Map<MapKey, Value> value;
        
        public MapValue(Map<MapKey, Value> value) {
            this.value = Map.copyOf(Objects.requireNonNull(value, "Map cannot be null"));
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.MAP; }
        
        @Override
        public String toString() {
            return value.toString();
        }
    }
    
    // Row Value
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class RowValue extends Value {
        private final ImprintRecord value;
        
        public RowValue(ImprintRecord value) {
            this.value = Objects.requireNonNull(value, "Record cannot be null");
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.ROW; }
        
        @Override
        public String toString() {
            return "Row{" + value + "}";
        }
    }
    
}