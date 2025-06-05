package com.imprint;

import com.imprint.core.*;
import com.imprint.types.*;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Imprint core functionality.
 */
public class IntegrationTest {

    @Test
    @DisplayName("Basic functionality: create, serialize, deserialize primitive types")
    void testBasicFunctionality() throws ImprintException {
        SchemaId schemaId = new SchemaId(1, 0xdeadbeef);
        var record = ImprintRecord.builder(schemaId)
                .field(1, 42)
                .field(2, "testing java imprint spec")
                .field(3, true)
                .field(4, 3.14159) // double
                .field(5, new byte[]{1, 2, 3, 4})
                .build();

        // Verify we can read values back using type getters
        assertEquals(42, record.getInt32(1));
        assertEquals("testing java imprint spec", record.getString(2));
        assertTrue(record.getBoolean(3));
        assertEquals(3.14159, record.getFloat64(4));
        assertArrayEquals(new byte[]{1,2,3,4}, record.getBytes(5));

        assertNull(record.getValue(999), "Non-existent field should return null from getValue()");
        assertThrows(ImprintException.class, () -> record.getInt32(999), "Accessing non-existent field with getInt32 should throw");

        // Test serialization round-trip
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);

        assertEquals(42, deserialized.getInt32(1));
        assertEquals("testing java imprint spec", deserialized.getString(2));
        assertTrue(deserialized.getBoolean(3));
        assertEquals(3.14159, deserialized.getFloat64(4));
        assertArrayEquals(new byte[]{1,2,3,4}, deserialized.getBytes(5));

        System.out.println("Basic functionality test passed");
    }

    @Test
    @DisplayName("Collections: create, serialize, deserialize arrays and maps")
    void testArraysAndMaps() throws ImprintException {
        SchemaId schemaId = new SchemaId(2, 0xcafebabe);

        // Create an array using builder for convenience
        List<Object> sourceIntList = Arrays.asList(1, 2, 3);

        // Create a map
        Map<String, Integer> sourceStringToIntMap = new HashMap<>();
        sourceStringToIntMap.put("one", 1);
        sourceStringToIntMap.put("two", 2);
        var record = ImprintRecord.builder(schemaId)
                .field(1, sourceIntList) // Builder converts List<Object> to List<Value>
                .field(2, sourceStringToIntMap) // Builder converts Map<Object, Object>
                .build();

        // Test serialization round-trip
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);

        // Verify array
        List<Value> deserializedArray = deserialized.getArray(1);
        assertNotNull(deserializedArray);
        assertEquals(3, deserializedArray.size());
        assertEquals(Value.fromInt32(1), deserializedArray.get(0));
        assertEquals(Value.fromInt32(2), deserializedArray.get(1));
        assertEquals(Value.fromInt32(3), deserializedArray.get(2));

        // Verify map
        Map<MapKey, Value> deserializedMap = deserialized.getMap(2);
        assertNotNull(deserializedMap);
        assertEquals(2, deserializedMap.size());
        assertEquals(Value.fromInt32(1), deserializedMap.get(MapKey.fromString("one")));
        assertEquals(Value.fromInt32(2), deserializedMap.get(MapKey.fromString("two")));

        System.out.println("Arrays and maps test passed");
    }

    @Test
    @DisplayName("Nested Records: create, serialize, deserialize records within records")
    void testNestedRecords() throws ImprintException {
        System.out.println("Testing nested records...");

        var innerSchemaId = new SchemaId(3, 0x12345678);
        var innerRecord = ImprintRecord.builder(innerSchemaId)
                .field(1, "nested data")
                .field(2, 9876543210L)
                .build();

        var outerSchemaId = new SchemaId(4, 0x87654321);
        var outerRecord = ImprintRecord.builder(outerSchemaId)
                .field(1, innerRecord) // Builder handles ImprintRecord directly
                .field(2, "outer data")
                .build();

        var buffer = outerRecord.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);

        assertEquals(4, deserialized.getHeader().getSchemaId().getFieldSpaceId());
        assertEquals("outer data", deserialized.getString(2));

        var nestedDeserialized = deserialized.getRow(1);
        assertNotNull(nestedDeserialized);
        assertEquals(3, nestedDeserialized.getHeader().getSchemaId().getFieldSpaceId());
        assertEquals("nested data", nestedDeserialized.getString(1));
        assertEquals(9876543210L, nestedDeserialized.getInt64(2));

        System.out.println("✓ Nested records test passed");
    }

    private ImprintRecord createTestRecordForGetters() throws ImprintException {
        SchemaId schemaId = new SchemaId(5, 0xabcdef01);

        List<Value> innerList1 = Arrays.asList(Value.fromInt32(10), Value.fromInt32(20));
        List<Value> innerList2 = Arrays.asList(Value.fromInt32(30), Value.fromInt32(40));
        List<Value> listOfLists = Arrays.asList(Value.fromArray(innerList1), Value.fromArray(innerList2));

        Map<MapKey, Value> mapWithArrayValue = new HashMap<>();
        mapWithArrayValue.put(MapKey.fromString("list1"), Value.fromArray(innerList1));

        return ImprintRecord.builder(schemaId)
                .field(1, true)
                .field(2, 12345)
                .field(3, 9876543210L)
                .field(4, 3.14f)
                .field(5, 2.718281828)
                .field(6, "hello type world")
                .field(7, new byte[]{10, 20, 30})
                .nullField(8)
                .field(9, Value.fromArray(listOfLists))     // Array of Arrays (using Value directly for test setup)
                .field(10, Value.fromMap(mapWithArrayValue)) // Map with Array value
                .field(11, Collections.emptyList())          // Empty Array via builder
                .field(12, Collections.emptyMap())           // Empty Map via builder
                .build();
    }

    private ImprintRecord serializeAndDeserialize(ImprintRecord record) throws ImprintException {
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        return ImprintRecord.deserialize(serialized);
    }

    @Test
    @DisplayName("Type Getters: Basic primitive and String types")
    void testBasicTypeGetters() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        assertTrue(record.getBoolean(1));
        assertEquals(12345, record.getInt32(2));
        assertEquals(9876543210L, record.getInt64(3));
        assertEquals(3.14f, record.getFloat32(4));
        assertEquals(2.718281828, record.getFloat64(5));
        assertEquals("hello type world", record.getString(6));
        assertArrayEquals(new byte[]{10, 20, 30}, record.getBytes(7));
    }

    @Test
    @DisplayName("Type Getters: Array of Arrays")
    void testTypeGetterArrayOfArrays() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        List<Value> arrOfArr = record.getArray(9);
        assertNotNull(arrOfArr);
        assertEquals(2, arrOfArr.size());
        assertInstanceOf(Value.ArrayValue.class, arrOfArr.get(0));
        Value.ArrayValue firstInnerArray = (Value.ArrayValue) arrOfArr.get(0);
        assertEquals(2, firstInnerArray.getValue().size());
        assertEquals(Value.fromInt32(10), firstInnerArray.getValue().get(0));
        assertEquals(Value.fromInt32(20), firstInnerArray.getValue().get(1));

        assertInstanceOf(Value.ArrayValue.class, arrOfArr.get(1));
        Value.ArrayValue secondInnerArray = (Value.ArrayValue) arrOfArr.get(1);
        assertEquals(2, secondInnerArray.getValue().size());
        assertEquals(Value.fromInt32(30), secondInnerArray.getValue().get(0));
        assertEquals(Value.fromInt32(40), secondInnerArray.getValue().get(1));
    }

    @Test
    @DisplayName("Type Getters: Map with Array Value")
    void testTypeGetterMapWithArrayValue() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        Map<MapKey, Value> mapWithArr = record.getMap(10);
        assertNotNull(mapWithArr);
        assertEquals(1, mapWithArr.size());
        assertInstanceOf(Value.ArrayValue.class, mapWithArr.get(MapKey.fromString("list1")));
        Value.ArrayValue innerArray = (Value.ArrayValue) mapWithArr.get(MapKey.fromString("list1"));
        assertNotNull(innerArray);
        assertEquals(2, innerArray.getValue().size());
        assertEquals(Value.fromInt32(10), innerArray.getValue().get(0));
    }

    @Test
    @DisplayName("Type Getters: Empty Collections (Array and Map)")
    void testErgonomicGettersEmptyCollections() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        List<Value> emptyArr = record.getArray(11);
        assertNotNull(emptyArr);
        assertTrue(emptyArr.isEmpty());

        Map<MapKey, Value> emptyMap = record.getMap(12);
        assertNotNull(emptyMap);
        assertTrue(emptyMap.isEmpty());
    }

    @Test
    @DisplayName("Type Getters: Exception for Field Not Found")
    void testErgonomicGetterExceptionFieldNotFound() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        ImprintException ex = assertThrows(ImprintException.class, () -> record.getInt32(99));
        assertEquals(ErrorType.FIELD_NOT_FOUND, ex.getErrorType());
    }

    @Test
    @DisplayName("Type Getters: Exception for Null Field accessed as primitive")
    void testErgonomicGetterExceptionNullField() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        ImprintException ex = assertThrows(ImprintException.class, () -> record.getString(8));
        assertEquals(ErrorType.TYPE_MISMATCH, ex.getErrorType()); // getString throws TYPE_MISMATCH for null
        assertTrue(ex.getMessage().contains("Field 8 is NULL"));


        // Also test getValue for a null field returns Value.NullValue
        Value nullValueField = record.getValue(8);
        assertNotNull(nullValueField);
        assertInstanceOf(Value.NullValue.class, nullValueField, "Field 8 should be Value.NullValue");
    }

    @Test
    @DisplayName("Type Getters: Exception for Type Mismatch")
    void testErgonomicGetterExceptionTypeMismatch() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        ImprintException ex = assertThrows(ImprintException.class, () -> record.getInt32(6)); // Field 6 is a String
        assertEquals(ErrorType.TYPE_MISMATCH, ex.getErrorType());
    }

    @Test
    @DisplayName("Type Getters: Row (Nested Record)")
    void testErgonomicGetterRow() throws ImprintException {
        var innerSchemaId = new SchemaId(6, 0x12345678);
        var innerRecord = ImprintRecord.builder(innerSchemaId)
                .field(101, "nested string")
                .field(102, 999L)
                .build();

        var recordWithRow = ImprintRecord.builder(new SchemaId(7, 0x87654321))
                .field(201, innerRecord) // Using builder to add row
                .field(202, "outer field")
                .build();

        var deserializedWithRow = serializeAndDeserialize(recordWithRow);

        var retrievedRow = deserializedWithRow.getRow(201);
        assertNotNull(retrievedRow);
        assertEquals(innerSchemaId, retrievedRow.getHeader().getSchemaId());
        assertEquals("nested string", retrievedRow.getString(101));
        assertEquals(999L, retrievedRow.getInt64(102));
        assertEquals("outer field", deserializedWithRow.getString(202));
    }
}