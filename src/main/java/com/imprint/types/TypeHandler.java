package com.imprint.types;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Interface for handling type-specific serialization, deserialization, and size estimation.
 * Note that primitives are basically boxed here which could impact performance slightly
 * but having all the types in their own implementation helps keep things organized for now, especially
 * for dealing with and testing more complex types in the future.
 */
public interface TypeHandler {
    Value deserialize(ByteBuffer buffer) throws ImprintException;
    void serialize(Value value, ByteBuffer buffer) throws ImprintException;
    int estimateSize(Value value) throws ImprintException;

    // Static implementations for each type
    TypeHandler NULL = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) {
            return Value.nullValue();
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            // NULL values have no payload
        }
        
        @Override
        public int estimateSize(Value value) {
            return 0;
        }
    };
    
    TypeHandler BOOL = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 1) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for bool");
            }
            byte boolByte = buffer.get();
            if (boolByte == 0) return Value.fromBoolean(false);
            if (boolByte == 1) return Value.fromBoolean(true);
            throw new ImprintException(com.imprint.error.ErrorType.SCHEMA_ERROR, "Invalid boolean value: " + boolByte);
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            var boolValue = (Value.BoolValue) value;
            buffer.put((byte) (boolValue.getValue() ? 1 : 0));
        }
        
        @Override
        public int estimateSize(Value value) {
            return 1;
        }
    };
    
    TypeHandler INT32 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 4) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int32");
            }
            return Value.fromInt32(buffer.getInt());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            var int32Value = (Value.Int32Value) value;
            buffer.putInt(int32Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 4;
        }
    };
    
    TypeHandler INT64 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 8) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int64");
            }
            return Value.fromInt64(buffer.getLong());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            Value.Int64Value int64Value = (Value.Int64Value) value;
            buffer.putLong(int64Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 8;
        }
    };
    
    TypeHandler FLOAT32 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 4) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float32");
            }
            return Value.fromFloat32(buffer.getFloat());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            var float32Value = (Value.Float32Value) value;
            buffer.putFloat(float32Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 4;
        }
    };
    
    TypeHandler FLOAT64 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 8) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float64");
            }
            return Value.fromFloat64(buffer.getDouble());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            var float64Value = (Value.Float64Value) value;
            buffer.putDouble(float64Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 8;
        }
    };
    
    TypeHandler BYTES = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int length = lengthResult.getValue();
            if (buffer.remaining() < length) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for bytes value data after VarInt. Slice from readValueBytes is too short. Needed: " + length + ", available: " + buffer.remaining());
            }
            var bytesView = buffer.slice();
            bytesView.limit(length);
            buffer.position(buffer.position() + length);
            return Value.fromBytesBuffer(bytesView.asReadOnlyBuffer());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            if (value instanceof Value.BytesBufferValue) {
                Value.BytesBufferValue bufferValue = (Value.BytesBufferValue) value;
                var bytesBuffer = bufferValue.getBuffer();
                VarInt.encode(bytesBuffer.remaining(), buffer);
                buffer.put(bytesBuffer);
            } else {
                Value.BytesValue bytesValue = (Value.BytesValue) value;
                byte[] bytes = bytesValue.getValue();
                VarInt.encode(bytes.length, buffer);
                buffer.put(bytes);
            }
        }
        
        @Override
        public int estimateSize(Value value) {
            if (value instanceof Value.BytesBufferValue) {
                Value.BytesBufferValue bufferValue = (Value.BytesBufferValue) value;
                int length = bufferValue.getBuffer().remaining();
                return VarInt.encodedLength(length) + length;
            } else {
                byte[] bytes = ((Value.BytesValue) value).getValue();
                return VarInt.encodedLength(bytes.length) + bytes.length;
            }
        }
    };
    
    TypeHandler STRING = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult strLengthResult = VarInt.decode(buffer);
            int strLength = strLengthResult.getValue();
            if (buffer.remaining() < strLength) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for string value data after VarInt. Slice from readValueBytes is too short. Needed: " + strLength + ", available: " + buffer.remaining());
            }
            var stringBytesView = buffer.slice();
            stringBytesView.limit(strLength);
            buffer.position(buffer.position() + strLength);
            try {
                return Value.fromStringBuffer(stringBytesView);
            } catch (Exception e) {
                throw new ImprintException(ErrorType.INVALID_UTF8_STRING, "Invalid UTF-8 string or buffer issue: " + e.getMessage());
            }
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            if (value instanceof Value.StringBufferValue) {
                var bufferValue = (Value.StringBufferValue) value;
                var stringBuffer = bufferValue.getBuffer();
                VarInt.encode(stringBuffer.remaining(), buffer);
                buffer.put(stringBuffer);
            } else {
                var stringValue = (Value.StringValue) value;
                byte[] stringBytes = stringValue.getUtf8Bytes();
                VarInt.encode(stringBytes.length, buffer);
                buffer.put(stringBytes);
            }
        }

        @Override
        public int estimateSize(Value value) {
            if (value instanceof Value.StringBufferValue) {
                Value.StringBufferValue bufferValue = (Value.StringBufferValue) value;
                int length = bufferValue.getBuffer().remaining();
                return VarInt.encodedLength(length) + length;
            } else {
                Value.StringValue stringValue = (Value.StringValue) value;
                int utf8Length = stringValue.getUtf8Length();  // Uses cached bytes
                return VarInt.encodedLength(utf8Length) + utf8Length;
            }
        }
    };
    
    TypeHandler ARRAY = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int length = lengthResult.getValue();

            if (length == 0) {
                return Value.fromArray(Collections.emptyList());
            }

            if (buffer.remaining() < 1) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for ARRAY element type code.");
            }
            var elementType = TypeCode.fromByte(buffer.get());
            var elements = new ArrayList<Value>(length);
            var elementHandler = elementType.getHandler();

            //Let each element handler consume what it needs from the buffer
            for (int i = 0; i < length; i++) {
                var element = elementHandler.deserialize(buffer); //Handler advances buffer position
                elements.add(element);
            }

            return Value.fromArray(elements);
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) throws ImprintException {
            var arrayValue = (Value.ArrayValue) value;
            var elements = arrayValue.getValue();
            VarInt.encode(elements.size(), buffer);
            
            if (elements.isEmpty()) return;

            var elementType = elements.get(0).getTypeCode();
            buffer.put(elementType.getCode());
            var elementHandler = elementType.getHandler();
            for (var element : elements) {
                if (element.getTypeCode() != elementType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                        "Array elements must have same type code: " + 
                        element.getTypeCode() + " != " + elementType);
                }
                elementHandler.serialize(element, buffer);
            }
        }
        
        @Override
        public int estimateSize(Value value) throws ImprintException {
            var arrayValue = (Value.ArrayValue) value;
            var elements = arrayValue.getValue();
            int sizeOfLength = VarInt.encodedLength(elements.size());
            if (elements.isEmpty()) {
                return sizeOfLength;
            }
            int sizeOfElementTypeCode = 1;
            int arraySize = sizeOfLength + sizeOfElementTypeCode;
            var elementHandler = elements.get(0).getTypeCode().getHandler();
            for (var element : elements) {
                arraySize += elementHandler.estimateSize(element);
            }
            return arraySize;
        }
    };
    
    TypeHandler MAP = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int length = lengthResult.getValue();

            if (length == 0) {
                return Value.fromMap(Collections.emptyMap());
            }

            if (buffer.remaining() < 2) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for MAP key/value type codes.");
            }
            var keyType = TypeCode.fromByte(buffer.get());
            var valueType = TypeCode.fromByte(buffer.get());
            var map = new HashMap<MapKey, Value>(length);

            var keyHandler = keyType.getHandler();
            var valueHandler = valueType.getHandler();

            //Let handlers consume directly from buffer
            for (int i = 0; i < length; i++) {
                var keyValue = keyHandler.deserialize(buffer);// Advances buffer
                var key = MapKey.fromValue(keyValue);

                var mapInternalValue = valueHandler.deserialize(buffer);//Advances buffer

                map.put(key, mapInternalValue);
            }

            return Value.fromMap(map);
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) throws ImprintException {
            var mapValue = (Value.MapValue) value;
            var map = mapValue.getValue();
            VarInt.encode(map.size(), buffer);
            
            if (map.isEmpty()) {
                return;
            }
            
            var iterator = map.entrySet().iterator();
            var first = iterator.next();
            var keyType = first.getKey().getTypeCode();
            var valueType = first.getValue().getTypeCode();
            
            buffer.put(keyType.getCode());
            buffer.put(valueType.getCode());
            
            serializeMapKey(first.getKey(), buffer);
            first.getValue().getTypeCode().getHandler().serialize(first.getValue(), buffer);
            
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.getKey().getTypeCode() != keyType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                        "Map keys must have same type code: " +
                                entry.getKey().getTypeCode() + " != " + keyType);
                }
                if (entry.getValue().getTypeCode() != valueType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                        "Map values must have same type code: " +
                                entry.getValue().getTypeCode() + " != " + valueType);
                }
                
                serializeMapKey(entry.getKey(), buffer);
                entry.getValue().getTypeCode().getHandler().serialize(entry.getValue(), buffer);
            }
        }
        
        @Override
        public int estimateSize(Value value) throws ImprintException {
            var mapValue = (Value.MapValue) value;
            var map = mapValue.getValue();
            int sizeOfLength = VarInt.encodedLength(map.size());
            if (map.isEmpty()) {
                return sizeOfLength;
            }
            int sizeOfTypeCodes = 2; 
            int mapSize = sizeOfLength + sizeOfTypeCodes; 
            
            for (var entry : map.entrySet()) {
                mapSize += estimateMapKeySize(entry.getKey());
                mapSize += entry.getValue().getTypeCode().getHandler().estimateSize(entry.getValue());
            }
            return mapSize;
        }

        private void serializeMapKey(MapKey key, ByteBuffer buffer) throws ImprintException {
            switch (key.getTypeCode()) {
                case INT32:
                    MapKey.Int32Key int32Key = (MapKey.Int32Key) key;
                    buffer.putInt(int32Key.getValue());
                    break;
                    
                case INT64:
                    MapKey.Int64Key int64Key = (MapKey.Int64Key) key;
                    buffer.putLong(int64Key.getValue());
                    break;
                    
                case BYTES:
                    MapKey.BytesKey bytesKey = (MapKey.BytesKey) key;
                    byte[] bytes = bytesKey.getValue();
                    VarInt.encode(bytes.length, buffer);
                    buffer.put(bytes);
                    break;
                    
                case STRING:
                    MapKey.StringKey stringKey = (MapKey.StringKey) key;
                    byte[] stringBytes = stringKey.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    VarInt.encode(stringBytes.length, buffer);
                    buffer.put(stringBytes);
                    break;
                    
                default:
                    throw new ImprintException(ErrorType.SERIALIZATION_ERROR, 
                        "Invalid map key type: " + key.getTypeCode());
            }
        }
        
        private int estimateMapKeySize(MapKey key) throws ImprintException {
            switch (key.getTypeCode()) {
                case INT32: return 4;
                case INT64: return 8;
                case BYTES:
                    byte[] bytes = ((MapKey.BytesKey) key).getValue();
                    return VarInt.encodedLength(bytes.length) + bytes.length;

                case STRING:
                    var str = ((MapKey.StringKey) key).getValue();
                    int utf8Length = str.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                    return VarInt.encodedLength(utf8Length) + utf8Length;

                default:
                    throw new ImprintException(ErrorType.SERIALIZATION_ERROR, 
                        "Invalid map key type: " + key.getTypeCode());
            }
        }
    };
}