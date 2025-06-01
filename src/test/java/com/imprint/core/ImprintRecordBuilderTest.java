package com.imprint.core;

import com.imprint.error.ImprintException;
import com.imprint.types.Value;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

class ImprintRecordBuilderTest {
    
    private static final SchemaId TEST_SCHEMA = new SchemaId(1, 0x12345678);
    
    @Test
    void shouldCreateRecordWithPrimitiveTypes() throws ImprintException {
        var record = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, true)
            .field(2, 42)
            .field(3, 123L)
            .field(4, 3.14f)
            .field(5, 2.718)
            .field(6, "hello world")
            .field(7, new byte[]{1, 2, 3})
            .nullField(8)
            .build();
        
        assertThat(record.getHeader().getSchemaId()).isEqualTo(TEST_SCHEMA);
        assertThat(record.getDirectory()).hasSize(8);
        
        // Verify field values
        assertThat(getFieldValue(record, 1, Value.BoolValue.class).getValue()).isTrue();
        assertThat(getFieldValue(record, 2, Value.Int32Value.class).getValue()).isEqualTo(42);
        assertThat(getFieldValue(record, 3, Value.Int64Value.class).getValue()).isEqualTo(123L);
        assertThat(getFieldValue(record, 4, Value.Float32Value.class).getValue()).isEqualTo(3.14f);
        assertThat(getFieldValue(record, 5, Value.Float64Value.class).getValue()).isEqualTo(2.718);
        assertThat(getStringValue(record, 6)).isEqualTo("hello world");
        assertThat(getBytesValue(record, 7)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(record.getValue(8).get()).isInstanceOf(Value.NullValue.class);
    }
    
    @Test
    void shouldCreateRecordWithCollections() throws ImprintException {
        var list = List.of(1, 2, 3);
        var map = Map.of("key1", 100, "key2", 200);
        
        var record = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, list)
            .field(2, map)
            .build();
        
        // Verify array
        var arrayValue = getFieldValue(record, 1, Value.ArrayValue.class);
        assertThat(arrayValue.getValue()).hasSize(3);
        assertThat(((Value.Int32Value) arrayValue.getValue().get(0)).getValue()).isEqualTo(1);
        assertThat(((Value.Int32Value) arrayValue.getValue().get(1)).getValue()).isEqualTo(2);
        assertThat(((Value.Int32Value) arrayValue.getValue().get(2)).getValue()).isEqualTo(3);
        
        // Verify map
        var mapValue = getFieldValue(record, 2, Value.MapValue.class);
        assertThat(mapValue.getValue()).hasSize(2);
    }
    
    @Test
    void shouldCreateRecordWithNestedRecord() throws ImprintException {
        var nestedRecord = ImprintRecord.builder(new SchemaId(2, 0x87654321))
            .field(1, "nested")
            .field(2, 999)
            .build();
        
        var record = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, "parent")
            .field(2, nestedRecord)
            .build();
        
        var rowValue = getFieldValue(record, 2, Value.RowValue.class);
        var nested = rowValue.getValue();
        assertThat(getStringValue(nested, 1)).isEqualTo("nested");
        assertThat(getFieldValue(nested, 2, Value.Int32Value.class).getValue()).isEqualTo(999);
    }
    
    @Test
    void shouldSupportConditionalFields() throws ImprintException {
        boolean includeOptional = true;
        String optionalValue = "optional";
        
        var record = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, "required")
            .fieldIf(includeOptional, 2, optionalValue)
            .fieldIfNotNull(3, null) // Should not add field
            .fieldIfNotNull(4, "not null") // Should add field
            .build();
        
        assertThat(record.getDirectory()).hasSize(3); // Only fields 1, 2, 4
        assertThat(getStringValue(record, 1)).isEqualTo("required");
        assertThat(getStringValue(record, 2)).isEqualTo("optional");
        assertThat(record.getValue(3)).isEmpty(); // Not added
        assertThat(getStringValue(record, 4)).isEqualTo("not null");
    }
    
    @Test
    void shouldSupportBulkOperations() throws ImprintException {
        var fieldsMap = Map.of(
            1, "bulk1",
            2, 42,
            3, true
        );
        
        var record = ImprintRecord.builder(TEST_SCHEMA)
            .fields(fieldsMap)
            .field(4, "additional")
            .build();
        
        assertThat(record.getDirectory()).hasSize(4);
        assertThat(getStringValue(record, 1)).isEqualTo("bulk1");
        assertThat(getFieldValue(record, 2, Value.Int32Value.class).getValue()).isEqualTo(42);
        assertThat(getFieldValue(record, 3, Value.BoolValue.class).getValue()).isTrue();
        assertThat(getStringValue(record, 4)).isEqualTo("additional");
    }
    
    @Test
    void shouldProvideBuilderUtilities() {
        var builder = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, "test")
            .field(2, 42);
        
        assertThat(builder.hasField(1)).isTrue();
        assertThat(builder.hasField(3)).isFalse();
        assertThat(builder.fieldCount()).isEqualTo(2);
        assertThat(builder.fieldIds()).containsExactly(1, 2);
    }
    
    @Test
    void shouldSupportAlternativeSchemaConstructor() throws ImprintException {
        var record = ImprintRecord.builder(1, 0x12345678)
            .field(1, "test")
            .build();
        
        assertThat(record.getHeader().getSchemaId().getFieldspaceId()).isEqualTo(1);
        assertThat(record.getHeader().getSchemaId().getSchemaHash()).isEqualTo(0x12345678);
    }
    
    @Test
    void shouldRoundTripThroughSerialization() throws ImprintException {
        var original = ImprintRecord.builder(TEST_SCHEMA)
            .field(1, "test string")
            .field(2, 42)
            .field(3, 3.14159)
            .field(4, true)
            .field(5, new byte[]{0x01, 0x02, 0x03})
            .build();
        
        var serialized = original.serializeToBuffer();
        var deserialized = ImprintRecord.deserialize(serialized);
        
        assertThat(deserialized.getHeader().getSchemaId()).isEqualTo(TEST_SCHEMA);
        assertThat(getStringValue(deserialized, 1)).isEqualTo("test string");
        assertThat(getFieldValue(deserialized, 2, Value.Int32Value.class).getValue()).isEqualTo(42);
        assertThat(getFieldValue(deserialized, 3, Value.Float64Value.class).getValue()).isEqualTo(3.14159);
        assertThat(getFieldValue(deserialized, 4, Value.BoolValue.class).getValue()).isTrue();
        assertThat(getBytesValue(deserialized, 5)).isEqualTo(new byte[]{0x01, 0x02, 0x03});
    }
    
    // Error cases
    
    @Test
    void shouldRejectDuplicateFieldIds() {
        assertThatThrownBy(() -> 
            ImprintRecord.builder(TEST_SCHEMA)
                .field(1, "first")
                .field(1, "duplicate") // Same field ID
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("Field ID 1 already exists");
    }
    
    @Test
    void shouldRejectEmptyRecord() {
        assertThatThrownBy(() -> 
            ImprintRecord.builder(TEST_SCHEMA).build()
        ).isInstanceOf(ImprintException.class)
         .hasMessageContaining("Cannot build empty record");
    }
    
    @Test 
    void shouldRejectInvalidMapKeys() {
        var mapWithInvalidKey = Map.of(3.14, "value"); // Double key not supported
        
        assertThatThrownBy(() -> 
            ImprintRecord.builder(TEST_SCHEMA)
                .field(1, mapWithInvalidKey)
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("Invalid map key type: Double");
    }
    
    @Test
    void shouldRejectNullValueWithoutExplicitNullField() {
        assertThatThrownBy(() -> 
            ImprintRecord.builder(TEST_SCHEMA)
                .field(1, (Value) null)
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("Value cannot be null - use nullField()");
    }
    
    // Helper methods for cleaner test assertions
    
    private <T extends Value> T getFieldValue(ImprintRecord record, int fieldId, Class<T> valueType) throws ImprintException {
        var value = record.getValue(fieldId);
        assertThat(value).isPresent();
        assertThat(value.get()).isInstanceOf(valueType);
        return valueType.cast(value.get());
    }
    
    private String getStringValue(ImprintRecord record, int fieldId) throws ImprintException {
        var value = record.getValue(fieldId).get();
        if (value instanceof Value.StringValue) {
            return ((Value.StringValue) value).getValue();
        } else if (value instanceof Value.StringBufferValue) {
            return ((Value.StringBufferValue) value).getValue();
        } else {
            throw new AssertionError("Expected string value, got: " + value.getClass());
        }
    }
    
    private byte[] getBytesValue(ImprintRecord record, int fieldId) throws ImprintException {
        var value = record.getValue(fieldId).get();
        if (value instanceof Value.BytesValue) {
            return ((Value.BytesValue) value).getValue();
        } else if (value instanceof Value.BytesBufferValue) {
            return ((Value.BytesBufferValue) value).getValue();
        } else {
            throw new AssertionError("Expected bytes value, got: " + value.getClass());
        }
    }
}