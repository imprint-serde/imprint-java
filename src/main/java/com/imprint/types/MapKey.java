package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

/**
 * A subset of Value that's valid as a map key.
 * Only Int32, Int64, Bytes, and String are valid map keys.
 */
public abstract class MapKey {
    
    public abstract TypeCode getTypeCode();
    public abstract boolean equals(Object obj);
    public abstract int hashCode();
    public abstract String toString();
    
    public static MapKey fromInt32(int value) {
        return new Int32Key(value);
    }
    
    public static MapKey fromInt64(long value) {
        return new Int64Key(value);
    }
    
    public static MapKey fromBytes(byte[] value) {
        return new BytesKey(value);
    }
    
    public static MapKey fromString(String value) {
        return new StringKey(value);
    }
    
    /**
     * Create MapKey from primitive object and type code (optimized, no Value objects).
     */
    public static MapKey fromPrimitive(TypeCode typeCode, Object primitiveValue) throws ImprintException {
        switch (typeCode) {
            case INT32:
                return fromInt32((Integer) primitiveValue);
            case INT64:
                return fromInt64((Long) primitiveValue);
            case BYTES:
                return fromBytes((byte[]) primitiveValue);
            case STRING:
                return fromString((String) primitiveValue);
            default:
                throw new ImprintException(ErrorType.TYPE_MISMATCH, "Cannot convert " + typeCode + " to MapKey");
        }
    }
    
    
    /**
     * Get the primitive value as Object (optimized, no Value objects).
     */
    public Object getPrimitiveValue() {
        switch (getTypeCode()) {
            case INT32:
                return ((Int32Key) this).getValue();
            case INT64:
                return ((Int64Key) this).getValue();
            case BYTES:
                return ((BytesKey) this).getValue();
            case STRING:
                return ((StringKey) this).getValue();
            default:
                throw new IllegalStateException("Unknown MapKey type: " + getTypeCode());
        }
    }
    
    
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Int32Key extends MapKey {
        private final int value;
        
        public Int32Key(int value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.INT32; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class Int64Key extends MapKey {
        private final long value;
        
        public Int64Key(long value) {
            this.value = value;
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.INT64; }
        
        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
    
    public static class BytesKey extends MapKey {
        private final byte[] value;
        
        public BytesKey(byte[] value) {
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
            if (obj == null || getClass() != obj.getClass()) return false;
            BytesKey that = (BytesKey) obj;
            return Arrays.equals(value, that.value);
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
    
    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class StringKey extends MapKey {
        private final String value;
        
        public StringKey(String value) {
            this.value = Objects.requireNonNull(value, "String cannot be null");
        }

        @Override
        public TypeCode getTypeCode() { return TypeCode.STRING; }
        
        @Override
        public String toString() {
            return "\"" + value + "\"";
        }
    }
}