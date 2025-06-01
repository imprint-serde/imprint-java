package com.imprint.types;

import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Interface for handling type-specific serialization, deserialization, and size estimation.
 * Note that primitives are potentially auto/un-boxed here which could impact performance slightly
 * but having all the types in their own implementation helps keep things organized for now, especially
 * for dealing with and testing more complex types in the future.
 */
public interface TypeHandler {
    Value deserialize(ByteBuffer buffer) throws ImprintException;
    void serialize(Value value, ByteBuffer buffer) throws ImprintException;
    int estimateSize(Value value);
    ByteBuffer readValueBytes(ByteBuffer buffer) throws ImprintException;
    
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
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
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
            Value.BoolValue boolValue = (Value.BoolValue) value;
            buffer.put((byte) (boolValue.getValue() ? 1 : 0));
        }
        
        @Override
        public int estimateSize(Value value) {
            return 1;
        }
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            var boolBuffer = buffer.slice();
            boolBuffer.limit(1);
            buffer.position(buffer.position() + 1);
            return boolBuffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler INT32 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 4) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int32");
            }
            return Value.fromInt32(buffer.getInt());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            Value.Int32Value int32Value = (Value.Int32Value) value;
            buffer.putInt(int32Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 4;
        }
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            var int32Buffer = buffer.slice();
            int32Buffer.limit(4);
            buffer.position(buffer.position() + 4);
            return int32Buffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler INT64 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 8) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for int64");
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
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            var int64Buffer = buffer.slice();
            int64Buffer.limit(8);
            buffer.position(buffer.position() + 8);
            return int64Buffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler FLOAT32 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 4) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float32");
            }
            return Value.fromFloat32(buffer.getFloat());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            Value.Float32Value float32Value = (Value.Float32Value) value;
            buffer.putFloat(float32Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 4;
        }
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            var float32Buffer = buffer.slice();
            float32Buffer.limit(4);
            buffer.position(buffer.position() + 4);
            return float32Buffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler FLOAT64 = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            if (buffer.remaining() < 8) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for float64");
            }
            return Value.fromFloat64(buffer.getDouble());
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            Value.Float64Value float64Value = (Value.Float64Value) value;
            buffer.putDouble(float64Value.getValue());
        }
        
        @Override
        public int estimateSize(Value value) {
            return 8;
        }
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) {
            var float64Buffer = buffer.slice();
            float64Buffer.limit(8);
            buffer.position(buffer.position() + 8);
            return float64Buffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler BYTES = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int length = lengthResult.getValue();
            if (buffer.remaining() < length) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for bytes value");
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
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) throws ImprintException {
            int originalPosition = buffer.position();
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int totalLength = lengthResult.getBytesRead() + lengthResult.getValue();
            buffer.position(originalPosition);
            var valueBuffer = buffer.slice();
            valueBuffer.limit(totalLength);
            buffer.position(buffer.position() + totalLength);
            return valueBuffer.asReadOnlyBuffer();
        }
    };
    
    TypeHandler STRING = new TypeHandler() {
        @Override
        public Value deserialize(ByteBuffer buffer) throws ImprintException {
            VarInt.DecodeResult strLengthResult = VarInt.decode(buffer);
            int strLength = strLengthResult.getValue();
            if (buffer.remaining() < strLength) {
                throw new ImprintException(com.imprint.error.ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for string value");
            }
            var stringBytesView = buffer.slice();
            stringBytesView.limit(strLength);
            buffer.position(buffer.position() + strLength);
            try {
                return Value.fromStringBuffer(stringBytesView.asReadOnlyBuffer());
            } catch (Exception e) {
                throw new ImprintException(com.imprint.error.ErrorType.INVALID_UTF8_STRING, "Invalid UTF-8 string");
            }
        }
        
        @Override
        public void serialize(Value value, ByteBuffer buffer) {
            if (value instanceof Value.StringBufferValue) {
                Value.StringBufferValue bufferValue = (Value.StringBufferValue) value;
                var stringBuffer = bufferValue.getBuffer();
                VarInt.encode(stringBuffer.remaining(), buffer);
                buffer.put(stringBuffer);
            } else {
                Value.StringValue stringValue = (Value.StringValue) value;
                byte[] stringBytes = stringValue.getValue().getBytes(StandardCharsets.UTF_8);
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
                String str = ((Value.StringValue) value).getValue();
                int utf8Length = str.getBytes(StandardCharsets.UTF_8).length;
                return VarInt.encodedLength(utf8Length) + utf8Length;
            }
        }
        
        @Override
        public ByteBuffer readValueBytes(ByteBuffer buffer) throws ImprintException {
            int originalPosition = buffer.position();
            VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
            int totalLength = lengthResult.getBytesRead() + lengthResult.getValue();
            buffer.position(originalPosition);
            var valueBuffer = buffer.slice();
            valueBuffer.limit(totalLength);
            buffer.position(buffer.position() + totalLength);
            return valueBuffer.asReadOnlyBuffer();
        }
    };
}