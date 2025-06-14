package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Static serialization methods for all Imprint types.
 * Eliminates virtual dispatch overhead from TypeHandler interface.
 */
@UtilityClass
public final class ImprintSerializers {

    
    // Primitive serializers
    public static void serializeBool(boolean value, ByteBuffer buffer) {
        buffer.put((byte) (value ? 1 : 0));
    }
    
    public static void serializeInt32(int value, ByteBuffer buffer) {
        buffer.putInt(value);
    }
    
    public static void serializeInt64(long value, ByteBuffer buffer) {
        buffer.putLong(value);
    }
    
    public static void serializeFloat32(float value, ByteBuffer buffer) {
        buffer.putFloat(value);
    }
    
    public static void serializeFloat64(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }
    
    public static void serializeString(String value, ByteBuffer buffer) {
        byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
        VarInt.encode(utf8Bytes.length, buffer);
        buffer.put(utf8Bytes);
    }
    
    public static void serializeBytes(byte[] value, ByteBuffer buffer) {
        VarInt.encode(value.length, buffer);
        buffer.put(value);
    }
    
    public static void serializeArray(java.util.List<?> list, ByteBuffer buffer, 
                                     java.util.function.Function<Object, TypeCode> typeConverter,
                                     java.util.function.BiConsumer<Object, ByteBuffer> elementSerializer) throws ImprintException {
        VarInt.encode(list.size(), buffer);
        
        if (list.isEmpty()) return; // Empty arrays don't need type code

        // Convert first element to determine element type
        Object firstElement = list.get(0);
        TypeCode firstTypeCode = typeConverter.apply(firstElement);
        buffer.put(firstTypeCode.getCode());
        
        // Serialize all elements - they must be same type
        for (Object element : list) {
            TypeCode elementTypeCode = typeConverter.apply(element);
            if (elementTypeCode != firstTypeCode) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR,
                        "Array elements must have same type");
            }
            elementSerializer.accept(element, buffer);
        }
    }
    
    public static void serializeMap(java.util.Map<?, ?> map, ByteBuffer buffer,
                                   java.util.function.Function<Object, MapKey> keyConverter,
                                   java.util.function.Function<Object, TypeCode> typeConverter,
                                   java.util.function.BiConsumer<Object, ByteBuffer> valueSerializer) throws ImprintException {
        VarInt.encode(map.size(), buffer);
        
        if (map.isEmpty()) return;
        
        var iterator = map.entrySet().iterator();
        var first = iterator.next();
        
        // Convert key and value to determine types
        MapKey firstKey = keyConverter.apply(first.getKey());
        TypeCode firstValueType = typeConverter.apply(first.getValue());
        
        buffer.put(firstKey.getTypeCode().getCode());
        buffer.put(firstValueType.getCode());
        
        // Serialize first pair
        serializeMapKeyDirect(firstKey, buffer);
        valueSerializer.accept(first.getValue(), buffer);
        
        // Serialize remaining pairs
        while (iterator.hasNext()) {
            var entry = iterator.next();
            MapKey key = keyConverter.apply(entry.getKey());
            TypeCode valueType = typeConverter.apply(entry.getValue());
            
            if (key.getTypeCode() != firstKey.getTypeCode()) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR,
                        "Map keys must have same type");
            }
            if (valueType != firstValueType) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR,
                        "Map values must have same type");
            }
            
            serializeMapKeyDirect(key, buffer);
            valueSerializer.accept(entry.getValue(), buffer);
        }
    }
    
    private static void serializeMapKeyDirect(MapKey key, ByteBuffer buffer) throws ImprintException {
        switch (key.getTypeCode()) {
            case INT32:
                buffer.putInt(((MapKey.Int32Key) key).getValue());
                break;
            case INT64:
                buffer.putLong(((MapKey.Int64Key) key).getValue());
                break;
            case BYTES:
                byte[] bytes = ((MapKey.BytesKey) key).getValue();
                VarInt.encode(bytes.length, buffer);
                buffer.put(bytes);
                break;
            case STRING:
                String str = ((MapKey.StringKey) key).getValue();
                byte[] stringBytes = str.getBytes(StandardCharsets.UTF_8);
                VarInt.encode(stringBytes.length, buffer);
                buffer.put(stringBytes);
                break;
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR,
                        "Invalid map key type: " + key.getTypeCode());
        }
    }

    @SuppressWarnings("unused")
    public static void serializeNull(ByteBuffer buffer) {
        // NULL values have no payload data
    }

    // Fast size estimation using heuristics
    public static int estimateSize(TypeCode typeCode, Object value) {
        switch (typeCode) {
            case NULL: return 0;
            case BOOL: return 1;
            case INT32:
            case FLOAT32:
                return 4;
            case INT64:
            case FLOAT64:
                return 8;
            case STRING:
                String str = (String) value;
                return str.length() > 1000 ? 5 + str.length() * 3 : 256;
            case BYTES:
                byte[] bytes = (byte[]) value;
                return bytes.length > 1000 ? 5 + bytes.length : 256;
            case ARRAY:
                return 512; // Conservative: most arrays are < 512 bytes
            case MAP:
                return 512; // Conservative: most maps are < 512 bytes
            case ROW:
                return 1024; // Conservative: most nested records are < 1KB
            default:
                return 64; // Fallback
        }
    }
}