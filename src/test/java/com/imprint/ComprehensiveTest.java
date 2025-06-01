package com.imprint;

import com.imprint.core.*;
import com.imprint.types.*;
import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Comprehensive test to verify all functionality works correctly.
 */
public class ComprehensiveTest {
    
    public static void main(String[] args) {
        try {
            testVarIntFunctionality();
            testValueTypes();
            testMapKeys();
            testComplexSerialization();
            testErrorHandling();
            testByteBufferPerformance();
            System.out.println("All comprehensive tests passed!");
        } catch (Exception e) {
            System.err.println("Comprehensive test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    static void testVarIntFunctionality() throws ImprintException {
        System.out.println("Testing VarInt functionality...");
        
        // Test encoding/decoding of various values
        int[] testValues = {0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE};
        
        for (int value : testValues) {
            ByteBuffer buffer = ByteBuffer.allocate(10);
            VarInt.encode(value, buffer);
            int encodedLength = buffer.position();
            
            buffer.flip();
            VarInt.DecodeResult result = VarInt.decode(buffer);
            
            assert result.getValue() == value : "VarInt roundtrip failed for " + value;
            assert result.getBytesRead() == encodedLength : "Bytes read mismatch for " + value;
        }
        
        System.out.println("✓ VarInt functionality test passed");
    }
    
    static void testValueTypes() {
        System.out.println("Testing Value types");
        
        // Test all value types
        Value nullVal = Value.nullValue();
        Value boolVal = Value.fromBoolean(true);
        Value int32Val = Value.fromInt32(42);
        Value int64Val = Value.fromInt64(123456789L);
        Value float32Val = Value.fromFloat32(3.14f);
        Value float64Val = Value.fromFloat64(2.718281828);
        Value bytesVal = Value.fromBytes(new byte[]{1, 2, 3, 4});
        Value stringVal = Value.fromString("test");
        
        // Test type codes
        assert nullVal.getTypeCode() == TypeCode.NULL;
        assert boolVal.getTypeCode() == TypeCode.BOOL;
        assert int32Val.getTypeCode() == TypeCode.INT32;
        assert int64Val.getTypeCode() == TypeCode.INT64;
        assert float32Val.getTypeCode() == TypeCode.FLOAT32;
        assert float64Val.getTypeCode() == TypeCode.FLOAT64;
        assert bytesVal.getTypeCode() == TypeCode.BYTES;
        assert stringVal.getTypeCode() == TypeCode.STRING;
        
        // Test value extraction
        assert ((Value.BoolValue) boolVal).getValue();
        assert ((Value.Int32Value) int32Val).getValue() == 42;
        assert ((Value.Int64Value) int64Val).getValue() == 123456789L;
        assert ((Value.Float32Value) float32Val).getValue() == 3.14f;
        assert ((Value.Float64Value) float64Val).getValue() == 2.718281828;
        assert Arrays.equals(((Value.BytesValue) bytesVal).getValue(), new byte[]{1, 2, 3, 4});
        assert ((Value.StringValue) stringVal).getValue().equals("test");
        
        System.out.println("✓ Value types test passed");
    }
    
    static void testMapKeys() throws ImprintException {
        System.out.println("Testing MapKey functionality...");
        
        MapKey int32Key = MapKey.fromInt32(42);
        MapKey int64Key = MapKey.fromInt64(123L);
        MapKey bytesKey = MapKey.fromBytes(new byte[]{1, 2, 3});
        MapKey stringKey = MapKey.fromString("test");
        
        // Test conversion to/from Values
        Value int32Value = int32Key.toValue();
        Value int64Value = int64Key.toValue();
        Value bytesValue = bytesKey.toValue();
        Value stringValue = stringKey.toValue();
        
        assert MapKey.fromValue(int32Value).equals(int32Key);
        assert MapKey.fromValue(int64Value).equals(int64Key);
        assert MapKey.fromValue(bytesValue).equals(bytesKey);
        assert MapKey.fromValue(stringValue).equals(stringKey);
        
        System.out.println("✓ MapKey functionality test passed");
    }
    
    static void testComplexSerialization() throws ImprintException {
        System.out.println("Testing complex serialization...");
        
        SchemaId schemaId = new SchemaId(1, 0xdeadbeef);
        ImprintWriter writer = new ImprintWriter(schemaId);
        
        // Create complex nested structure
        List<Value> array = Arrays.asList(
            Value.fromInt32(1),
            Value.fromInt32(2),
            Value.fromInt32(3)
        );
        
        Map<MapKey, Value> map = new HashMap<>();
        map.put(MapKey.fromString("key1"), Value.fromString("value1"));
        map.put(MapKey.fromString("key2"), Value.fromString("value2"));
        
        writer.addField(1, Value.fromArray(array))
              .addField(2, Value.fromMap(map))
              .addField(3, Value.fromString("complex test"));
              
        ImprintRecord record = writer.build();
        
        // Test ByteBuffer serialization
        ByteBuffer serialized = record.serializeToBuffer();
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify complex structures
        Value deserializedArray = deserialized.getValue(1).get();
        assert deserializedArray instanceof Value.ArrayValue;
        List<Value> deserializedList = ((Value.ArrayValue) deserializedArray).getValue();
        assert deserializedList.size() == 3;
        assert deserializedList.get(0).equals(Value.fromInt32(1));
        
        Value deserializedMap = deserialized.getValue(2).get();
        assert deserializedMap instanceof Value.MapValue;
        Map<MapKey, Value> deserializedMapValue = ((Value.MapValue) deserializedMap).getValue();
        assert deserializedMapValue.size() == 2;
        assert deserializedMapValue.get(MapKey.fromString("key1")).equals(Value.fromString("value1"));
        
        System.out.println("✓ Complex serialization test passed");
    }
    
    static void testErrorHandling() {
        System.out.println("Testing error handling...");
        
        try {
            // Test invalid type code
            TypeCode.fromByte((byte) 0xFF);
            assert false : "Should have thrown exception for invalid type code";
        } catch (ImprintException e) {
            assert e.getErrorType() == com.imprint.error.ErrorType.INVALID_TYPE_CODE;
        }
        
        try {
            // Test invalid magic byte
            byte[] invalidData = new byte[15];
            invalidData[0] = 0x00; // wrong magic
            ImprintRecord.deserialize(invalidData);
            assert false : "Should have thrown exception for invalid magic";
        } catch (ImprintException e) {
            assert e.getErrorType() == com.imprint.error.ErrorType.INVALID_MAGIC;
        }
        
        System.out.println("✓ Error handling test passed");
    }
    
    static void testByteBufferPerformance() throws ImprintException {
        System.out.println("Testing ByteBuffer performance benefits...");
        
        // Create a record with moderate-sized data
        byte[] testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        
        SchemaId schemaId = new SchemaId(1, 0x12345678);
        ImprintWriter writer = new ImprintWriter(schemaId);
        writer.addField(1, Value.fromBytes(testData))
              .addField(2, Value.fromString("performance test"));
              
        ImprintRecord record = writer.build();
        
        // Test that raw bytes access is zero-copy
        Optional<ByteBuffer> rawBytes = record.getRawBytes(1);
        assert rawBytes.isPresent();
        assert rawBytes.get().isReadOnly();
        
        // Test ByteBuffer serialization
        ByteBuffer serialized = record.serializeToBuffer();
        assert serialized.isReadOnly();
        
        // Verify deserialization works
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);
        Value retrievedBytes = deserialized.getValue(1).get();
        assert Arrays.equals(((Value.BytesValue) retrievedBytes).getValue(), testData);
        
        System.out.println("✓ ByteBuffer performance test passed");
    }
}