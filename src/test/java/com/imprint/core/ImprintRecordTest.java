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
        var writer = new ImprintWriter(schemaId);
        
        writer.addField(1, Value.fromInt32(42))
              .addField(2, Value.fromString("hello"));

        var record = writer.build();
        
        assertThat(record.getHeader().getSchemaId()).isEqualTo(schemaId);
        assertThat(record.getDirectory()).hasSize(2);
        
        Optional<Value> field1 = record.getValue(1);
        Optional<Value> field2 = record.getValue(2);
        
        assertThat(field1).isPresent();
        assertThat(field1.get()).isInstanceOf(Value.Int32Value.class);
        assertThat(((Value.Int32Value) field1.get()).getValue()).isEqualTo(42);
        
        assertThat(field2).isPresent();
        assertThat(field2.get().getTypeCode()).isEqualTo(com.imprint.types.TypeCode.STRING);
        String stringValue = getStringValue(field2.get());
        assertThat(stringValue).isEqualTo("hello");
        
        // Non-existent field should return empty
        assertThat(record.getValue(999)).isEmpty();
    }
    
    @Test
    void shouldRoundtripThroughSerialization() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        var writer = new ImprintWriter(schemaId);
        
        writer.addField(1, Value.nullValue())
              .addField(2, Value.fromBoolean(true))
              .addField(3, Value.fromInt32(42))
              .addField(4, Value.fromInt64(123456789L))
              .addField(5, Value.fromFloat32(3.14f))
              .addField(6, Value.fromFloat64(2.718281828))
              .addField(7, Value.fromBytes(new byte[]{1, 2, 3, 4}))
              .addField(8, Value.fromString("test string"));

        var original = writer.build();
        
        // Serialize and deserialize
        var buffer = original.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify metadata
        assertThat(deserialized.getHeader().getSchemaId().getFieldspaceId()).isEqualTo(1);
        assertThat(deserialized.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xdeadbeef);
        assertThat(deserialized.getDirectory()).hasSize(8);
        
        // Verify all values
        assertThat(deserialized.getValue(1)).contains(Value.nullValue());
        assertThat(deserialized.getValue(2)).contains(Value.fromBoolean(true));
        assertThat(deserialized.getValue(3)).contains(Value.fromInt32(42));
        assertThat(deserialized.getValue(4)).contains(Value.fromInt64(123456789L));
        assertThat(deserialized.getValue(5)).contains(Value.fromFloat32(3.14f));
        assertThat(deserialized.getValue(6)).contains(Value.fromFloat64(2.718281828));
        assertThat(deserialized.getValue(7)).contains(Value.fromBytes(new byte[]{1, 2, 3, 4}));
        assertThat(deserialized.getValue(8)).contains(Value.fromString("test string"));
        
        // Non-existent field
        assertThat(deserialized.getValue(999)).isEmpty();
    }
    
    @Test
    void shouldHandleArrays() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        var writer = new ImprintWriter(schemaId);
        
        List<Value> intArray = Arrays.asList(
            Value.fromInt32(1),
            Value.fromInt32(2),
            Value.fromInt32(3)
        );
        
        writer.addField(1, Value.fromArray(intArray));
        ImprintRecord record = writer.build();
        
        // Serialize and deserialize
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        Optional<Value> arrayValue = deserialized.getValue(1);
        assertThat(arrayValue).isPresent();
        assertThat(arrayValue.get()).isInstanceOf(Value.ArrayValue.class);
        
        List<Value> deserializedArray = ((Value.ArrayValue) arrayValue.get()).getValue();
        assertThat(deserializedArray).hasSize(3);
        assertThat(deserializedArray.get(0)).isEqualTo(Value.fromInt32(1));
        assertThat(deserializedArray.get(1)).isEqualTo(Value.fromInt32(2));
        assertThat(deserializedArray.get(2)).isEqualTo(Value.fromInt32(3));
    }
    
    @Test
    void shouldHandleMaps() throws ImprintException {
        var schemaId = new SchemaId(1, 0xdeadbeef);
        var writer = new ImprintWriter(schemaId);
        
        var map = new HashMap<MapKey, Value>();
        map.put(MapKey.fromString("key1"), Value.fromInt32(1));
        map.put(MapKey.fromString("key2"), Value.fromInt32(2));
        
        writer.addField(1, Value.fromMap(map));
        var record = writer.build();
        
        // Serialize and deserialize
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        Optional<Value> mapValue = deserialized.getValue(1);
        assertThat(mapValue).isPresent();
        assertThat(mapValue.get()).isInstanceOf(Value.MapValue.class);
        
        Map<MapKey, Value> deserializedMap = ((Value.MapValue) mapValue.get()).getValue();
        assertThat(deserializedMap).hasSize(2);
        assertThat(deserializedMap.get(MapKey.fromString("key1"))).isEqualTo(Value.fromInt32(1));
        assertThat(deserializedMap.get(MapKey.fromString("key2"))).isEqualTo(Value.fromInt32(2));
    }
    
    @Test
    void shouldHandleNestedRecords() throws ImprintException {
        // Create inner record
        var innerSchemaId = new SchemaId(2, 0xcafebabe);
        var innerWriter = new ImprintWriter(innerSchemaId);
        innerWriter.addField(1, Value.fromInt32(42))
                   .addField(2, Value.fromString("nested"));
        var innerRecord = innerWriter.build();
        
        // Create outer record containing inner record
        var outerSchemaId = new SchemaId(1, 0xdeadbeef);
        var outerWriter = new ImprintWriter(outerSchemaId);
        outerWriter.addField(1, Value.fromRow(innerRecord))
                   .addField(2, Value.fromInt64(123L));
        var outerRecord = outerWriter.build();
        
        // Serialize and deserialize
        var buffer = outerRecord.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify outer record metadata
        assertThat(deserialized.getHeader().getSchemaId().getFieldspaceId()).isEqualTo(1);
        assertThat(deserialized.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xdeadbeef);
        
        // Verify nested record
        Optional<Value> rowValue = deserialized.getValue(1);
        assertThat(rowValue).isPresent();
        assertThat(rowValue.get()).isInstanceOf(Value.RowValue.class);

        var nestedRecord = ((Value.RowValue) rowValue.get()).getValue();
        assertThat(nestedRecord.getHeader().getSchemaId().getFieldspaceId()).isEqualTo(2);
        assertThat(nestedRecord.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0xcafebabe);
        
        assertThat(nestedRecord.getValue(1)).contains(Value.fromInt32(42));
        assertThat(nestedRecord.getValue(2)).contains(Value.fromString("nested"));
        
        // Verify outer record field
        assertThat(deserialized.getValue(2)).contains(Value.fromInt64(123L));
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
        var writer = new ImprintWriter(schemaId);
        
        // Add duplicate field IDs - last one should win
        writer.addField(1, Value.fromInt32(42))
              .addField(1, Value.fromInt32(43));

        var record = writer.build();
        
        assertThat(record.getDirectory()).hasSize(1);
        assertThat(record.getValue(1)).contains(Value.fromInt32(43));
    }
}