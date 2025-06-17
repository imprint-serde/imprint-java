package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.ImprintBuffer;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Static serialization methods for all Imprint types.
 * Eliminates virtual dispatch overhead from TypeHandler interface.
 */
@UtilityClass
public final class ImprintSerializers {

    @SuppressWarnings("unused")
    public static void serializeNull(ByteBuffer buffer) {
        // NULL values have no payload data so this is here really only for consistency
    }

    public static void serializeBool(boolean value, ImprintBuffer buffer) {
        buffer.putByte((byte) (value ? 1 : 0));
    }
    
    public static void serializeInt32(int value, ImprintBuffer buffer) {
        buffer.putInt(value);
    }
    
    public static void serializeInt64(long value, ImprintBuffer buffer) {
        buffer.putLong(value);
    }
    
    public static void serializeFloat32(float value, ImprintBuffer buffer) {
        buffer.putFloat(value);
    }
    
    public static void serializeFloat64(double value, ImprintBuffer buffer) {
        buffer.putDouble(value);
    }
    
    public static void serializeString(String value, ImprintBuffer buffer) {
        buffer.putString(value);
    }
    
    public static void serializeBytes(byte[] value, ImprintBuffer buffer) {
        buffer.putVarInt(value.length);
        buffer.putBytes(value);
    }
    
    public static void serializeArray(List<?> list, ImprintBuffer buffer, Function<Object, TypeCode> typeConverter, BiConsumer<Object, ImprintBuffer> elementSerializer)
            throws ImprintException {
        buffer.putVarInt(list.size());
        
        if (list.isEmpty())
            return; // Empty arrays technically don't need type code

        // Convert first element to determine element type
        var firstElement = list.get(0);
        var firstTypeCode = typeConverter.apply(firstElement);
        buffer.putByte(firstTypeCode.getCode());
        
        // Serialize all elements (must be homogenous collections)
        for (var element : list) {
            var elementTypeCode = typeConverter.apply(element);
            if (elementTypeCode != firstTypeCode) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, "Array elements must have same type");
            }
            elementSerializer.accept(element, buffer);
        }
    }
    
    public static void serializeMap(Map<?, ?> map, ImprintBuffer buffer, Function<Object, MapKey> keyConverter, Function<Object, TypeCode> typeConverter,
                                    BiConsumer<Object, ImprintBuffer> valueSerializer) throws ImprintException {
        buffer.putVarInt(map.size());
        
        if (map.isEmpty()) return;
        
        var iterator = map.entrySet().iterator();
        var first = iterator.next();
        
        // Convert key and value to determine types
        var firstKey = keyConverter.apply(first.getKey());
        var firstValueType = typeConverter.apply(first.getValue());
        
        buffer.putByte(firstKey.getTypeCode().getCode());
        buffer.putByte(firstValueType.getCode());
        
        // Serialize first pair
        serializeMapKeyDirect(firstKey, buffer);
        valueSerializer.accept(first.getValue(), buffer);
        
        // Serialize remaining pairs
        while (iterator.hasNext()) {
            var entry = iterator.next();
            var key = keyConverter.apply(entry.getKey());
            var valueType = typeConverter.apply(entry.getValue());
            
            if (key.getTypeCode() != firstKey.getTypeCode()) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, "Map keys must have same type");
            }
            if (valueType != firstValueType) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, "Map values must have same type");
            }
            
            serializeMapKeyDirect(key, buffer);
            valueSerializer.accept(entry.getValue(), buffer);
        }
    }
    
    private static void serializeMapKeyDirect(MapKey key, ImprintBuffer buffer) throws ImprintException {
        switch (key.getTypeCode()) {
            case INT32:
                buffer.putInt(((MapKey.Int32Key) key).getValue());
                break;
            case INT64:
                buffer.putLong(((MapKey.Int64Key) key).getValue());
                break;
            case BYTES:
                byte[] bytes = ((MapKey.BytesKey) key).getValue();
                buffer.putVarInt(bytes.length);
                buffer.putBytes(bytes);
                break;
            case STRING:
                String str = ((MapKey.StringKey) key).getValue();
                buffer.putString(str);
                break;
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Invalid map key type: " + key.getTypeCode());
        }
    }

    @SuppressWarnings("unused")
    public static void serializeNull(ImprintBuffer buffer) {
        // NULL values have no payload data
    }


    public static int estimateSize(TypeCode typeCode, Object value) {
        byte code = typeCode.getCode();
        if (code == TypeCode.INT32.getCode() || code == TypeCode.FLOAT32.getCode()) return 4;
        if (code == TypeCode.INT64.getCode() || code == TypeCode.FLOAT64.getCode()) return 8;
        if (code == TypeCode.BOOL.getCode()) return 1;
        if (code == TypeCode.NULL.getCode()) return 0;
        if (code == TypeCode.STRING.getCode()) {
            var str = (String) value;
            return str.length() > 1000 ? 5 + str.length() * 3 : 256;
        }
        if (code == TypeCode.BYTES.getCode()) {
            var bytes = (byte[]) value;
            return bytes.length > 1000 ? 5 + bytes.length : 256;
        }
        if (code == TypeCode.ARRAY.getCode() || code == TypeCode.MAP.getCode()) return 512;
        if (code == TypeCode.ROW.getCode()) return 1024;
        throw new IllegalArgumentException("Unknown TypeCode: " + typeCode);
    }
}