package com.imprint.core;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import com.imprint.types.Value;
import com.imprint.types.MapKey;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ImprintRecordTest {
    
    // Helper method to extract string value from either StringValue or StringBufferValue
    private String getStringValue(Value value) {
        if (value instanceof Value.StringValue) {
            return ((Value.StringValue) value).getValue();
        } else if (value instanceof Value.StringBufferValue) {
            return ((Value.StringBufferValue) value).getValue();
        } else {
            throw new IllegalArgumentException("Expected string value, got: " + value.getClass());
        }
    }
    
    @Test
    void shouldCreateSimpleRecord() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        var record = ImprintRecord.builder(schemaId)
              .field(1, Value.fromInt32(42))
              .field(2, Value.fromString("hello"))
              .build();
        
        assertThat(record.getHeader().getSchemaId()).isEqualTo(schemaId);
        assertThat(record.getDirectory()).hasSize(2);
        
        Value field1 = record.getValue(1);
        Value field2 = record.getValue(2);
        
        assertThat(field1).isNotNull();
        assertThat(field1).isInstanceOf(Value.Int32Value.class);
        assertThat(((Value.Int32Value) field1).getValue()).isEqualTo(42);
        
        assertThat(field2).isNotNull();
        assertThat(field2.getTypeCode()).isEqualTo(com.imprint.types.TypeCode.STRING);
        String stringValue = getStringValue(field2);
        assertThat(stringValue).isEqualTo("hello");
        
        // Non-existent field should return null
        assertThat(record.getValue(999)).isNull();
    }
    
    @Test
    void shouldRoundtripThroughSerialization() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        var original = ImprintRecord.builder(schemaId)
              .field(1, Value.nullValue())
              .field(2, Value.fromBoolean(true))
              .field(3, Value.fromInt32(42))
              .field(4, Value.fromInt64(123456789L))
              .field(5, Value.fromFloat32(3.14f))
              .field(6, Value.fromFloat64(2.718281828))
              .field(7, Value.fromBytes(new byte[]{1, 2, 3, 4}))
              .field(8, Value.fromString("test string"))
              .build();
        
        // Serialize and deserialize
        var buffer = original.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify metadata
        assertThat(deserialized.getHeader().getSchemaId().getFieldSpaceId()).isEqualTo(1);
        assertThat(deserialized.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xdeadbeef);
        assertThat(deserialized.getDirectory()).hasSize(8);
        
        // Verify all values
        assertThat(deserialized.getValue(1)).isEqualTo(Value.nullValue());
        assertThat(deserialized.getValue(2)).isEqualTo(Value.fromBoolean(true));
        assertThat(deserialized.getValue(3)).isEqualTo(Value.fromInt32(42));
        assertThat(deserialized.getValue(4)).isEqualTo(Value.fromInt64(123456789L));
        assertThat(deserialized.getValue(5)).isEqualTo(Value.fromFloat32(3.14f));
        assertThat(deserialized.getValue(6)).isEqualTo(Value.fromFloat64(2.718281828));
        assertThat(deserialized.getValue(7)).isEqualTo(Value.fromBytes(new byte[]{1, 2, 3, 4}));
        assertThat(deserialized.getValue(8)).isEqualTo(Value.fromString("test string"));
        
        // Non-existent field
        assertThat(deserialized.getValue(999)).isNull();
    }
    
    @Test
    void shouldHandleArrays() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        
        List<Value> intArray = Arrays.asList(
            Value.fromInt32(1),
            Value.fromInt32(2),
            Value.fromInt32(3)
        );
        
        var record = ImprintRecord.builder(schemaId)
            .field(1, Value.fromArray(intArray))
            .build();
        
        // Serialize and deserialize
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        Value arrayValue = deserialized.getValue(1);
        assertThat(arrayValue).isNotNull();
        assertThat(arrayValue).isInstanceOf(Value.ArrayValue.class);
        
        List<Value> deserializedArray = ((Value.ArrayValue) arrayValue).getValue();
        assertThat(deserializedArray).hasSize(3);
        assertThat(deserializedArray.get(0)).isEqualTo(Value.fromInt32(1));
        assertThat(deserializedArray.get(1)).isEqualTo(Value.fromInt32(2));
        assertThat(deserializedArray.get(2)).isEqualTo(Value.fromInt32(3));
    }
    
    @Test
    void shouldHandleMaps() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        
        var map = new HashMap<MapKey, Value>();
        map.put(MapKey.fromString("key1"), Value.fromInt32(1));
        map.put(MapKey.fromString("key2"), Value.fromInt32(2));
        
        var record = ImprintRecord.builder(schemaId)
            .field(1, Value.fromMap(map))
            .build();
        
        // Serialize and deserialize
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        Value mapValue = deserialized.getValue(1);
        assertThat(mapValue).isNotNull();
        assertThat(mapValue).isInstanceOf(Value.MapValue.class);
        
        Map<MapKey, Value> deserializedMap = ((Value.MapValue) mapValue).getValue();
        assertThat(deserializedMap).hasSize(2);
        assertThat(deserializedMap.get(MapKey.fromString("key1"))).isEqualTo(Value.fromInt32(1));
        assertThat(deserializedMap.get(MapKey.fromString("key2"))).isEqualTo(Value.fromInt32(2));
    }
    
    @Test
    void shouldHandleNestedRecords() throws ImprintException {
        // Create inner record
        var innerSchemaId = new SchemaId(2, 0xcafebabe);
        var innerRecord = ImprintRecord.builder(innerSchemaId)
                   .field(1, Value.fromInt32(42))
                   .field(2, Value.fromString("nested"))
                   .build();
        
        // Create outer record containing inner record
        var outerSchemaId = new SchemaId(1, 0xdeadbeef);
        var outerRecord = ImprintRecord.builder(outerSchemaId)
                   .field(1, Value.fromRow(innerRecord))
                   .field(2, Value.fromInt64(123L))
                   .build();
        
        // Serialize and deserialize
        var buffer = outerRecord.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify outer record metadata
        assertThat(deserialized.getHeader().getSchemaId().getFieldSpaceId()).isEqualTo(1);
        assertThat(deserialized.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xdeadbeef);
        
        // Verify nested record
        Value rowValue = deserialized.getValue(1);
        assertThat(rowValue).isNotNull();
        assertThat(rowValue).isInstanceOf(Value.RowValue.class);

        var nestedRecord = ((Value.RowValue) rowValue).getValue();
        assertThat(nestedRecord.getHeader().getSchemaId().getFieldSpaceId()).isEqualTo(2);
        assertThat(nestedRecord.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xcafebabe);
        
        assertThat(nestedRecord.getValue(1)).isEqualTo(Value.fromInt32(42));
        assertThat(nestedRecord.getValue(2)).isEqualTo(Value.fromString("nested"));
        
        // Verify outer record field
        assertThat(deserialized.getValue(2)).isEqualTo(Value.fromInt64(123L));
    }
    
    @Test
    void shouldRejectInvalidMagic() {
        byte[] invalidData = new byte[15];
        invalidData[0] = 0x00; // wrong magic
        
        assertThatThrownBy(() -> ImprintRecord.deserialize(invalidData))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.INVALID_MAGIC);
    }
    
    @Test
    void shouldRejectUnsupportedVersion() {
        byte[] invalidData = new byte[15];
        invalidData[0] = (byte) 0x49; // correct magic
        invalidData[1] = (byte) 0xFF; // wrong version
        
        assertThatThrownBy(() -> ImprintRecord.deserialize(invalidData))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.UNSUPPORTED_VERSION);
    }
    
    @Test
    void shouldHandleDuplicateFieldIds() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        
        // Add duplicate field IDs - last one should win
        var record = ImprintRecord.builder(schemaId)
              .field(1, Value.fromInt32(42))
              .field(1, Value.fromInt32(43))
              .build();
        
        assertThat(record.getDirectory()).hasSize(1);
        assertThat(record.getValue(1)).isEqualTo(Value.fromInt32(43));
    }
}