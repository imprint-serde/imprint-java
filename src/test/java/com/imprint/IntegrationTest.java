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
    }

    @Test
    @DisplayName("Nested Records: create, serialize, deserialize records within records")
    void testNestedRecords() throws ImprintException {
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
    }

    @Test
    @DisplayName("Project: subset of fields with serialization round-trip")
    void testProjectSubsetWithSerialization() throws ImprintException {
        var schemaId = new SchemaId(10, 0xabcd1234);
        var originalRecord = ImprintRecord.builder(schemaId)
                .field(1, 100)
                .field(2, "keep this field")
                .field(3, false)
                .field(4, "remove this field")
                .field(5, 42.5)
                .field(6, new byte[]{9, 8, 7})
                .build();

        // Project fields 1, 2, 5 (skip 3, 4, 6)
        var projected = originalRecord.project(1, 2, 5);

        assertEquals(3, projected.getDirectory().size());
        assertEquals(100, projected.getInt32(1));
        assertEquals("keep this field", projected.getString(2));
        assertEquals(42.5, projected.getFloat64(5));

        // Verify missing fields
        assertNull(projected.getValue(3));
        assertNull(projected.getValue(4));
        assertNull(projected.getValue(6));

        // Test serialization round-trip of projected record
        var buffer = projected.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);

        assertEquals(3, deserialized.getDirectory().size());
        assertEquals(100, deserialized.getInt32(1));
        assertEquals("keep this field", deserialized.getString(2));
        assertEquals(42.5, deserialized.getFloat64(5));
    }

    @Test
    @DisplayName("Project: complex data types (arrays, maps, nested records)")
    void testProjectComplexTypes() throws ImprintException {
        var schemaId = new SchemaId(11, 0xbeef4567);

        // Create nested record
        var nestedRecord = ImprintRecord.builder(new SchemaId(12, 0x11111111))
                .field(100, "nested value")
                .build();

        // Create homogeneous array (all strings)
        var testArray = Arrays.asList(Value.fromString("item1"), Value.fromString("item2"), Value.fromString("item3"));

        // Create homogeneous map (string keys -> string values)
        var testMap = new HashMap<MapKey, Value>();
        testMap.put(MapKey.fromString("key1"), Value.fromString("value1"));
        testMap.put(MapKey.fromString("key2"), Value.fromString("value2"));

        var originalRecord = ImprintRecord.builder(schemaId)
                .field(1, "simple string")
                .field(2, Value.fromArray(testArray))
                .field(3, Value.fromMap(testMap))
                .field(4, nestedRecord)
                .field(5, 999L)
                .build();

        // Project only complex types
        var projected = originalRecord.project(2, 3, 4);

        assertEquals(3, projected.getDirectory().size());

        // Verify array projection (homogeneous strings)
        var projectedArray = projected.getArray(2);
        assertEquals(3, projectedArray.size());
        assertEquals(Value.fromString("item1"), projectedArray.get(0));
        assertEquals(Value.fromString("item2"), projectedArray.get(1));
        assertEquals(Value.fromString("item3"), projectedArray.get(2));

        // Verify map projection (string -> string)
        var projectedMap = projected.getMap(3);
        assertEquals(2, projectedMap.size());
        assertEquals(Value.fromString("value1"), projectedMap.get(MapKey.fromString("key1")));
        assertEquals(Value.fromString("value2"), projectedMap.get(MapKey.fromString("key2")));

        // Verify nested record projection
        var projectedNested = projected.getRow(4);
        assertEquals("nested value", projectedNested.getString(100));

        // Verify excluded fields
        assertNull(projected.getValue(1));
        assertNull(projected.getValue(5));
    }

    @Test
    @DisplayName("Merge: distinct fields with serialization round-trip")
    void testMergeDistinctFieldsWithSerialization() throws ImprintException {
        var schemaId = new SchemaId(20, 0xcafe5678);

        var record1 = ImprintRecord.builder(schemaId)
                .field(1, 100)
                .field(3, "from record1")
                .field(5, true)
                .build();

        var record2 = ImprintRecord.builder(schemaId)
                .field(2, 200L)
                .field(4, "from record2")
                .field(6, 3.14f)
                .build();

        var merged = record1.merge(record2);

        assertEquals(6, merged.getDirectory().size());
        assertEquals(100, merged.getInt32(1));
        assertEquals(200L, merged.getInt64(2));
        assertEquals("from record1", merged.getString(3));
        assertEquals("from record2", merged.getString(4));
        assertTrue(merged.getBoolean(5));
        assertEquals(3.14f, merged.getFloat32(6));

        // Test serialization round-trip of merged record
        var buffer = merged.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        var deserialized = ImprintRecord.deserialize(serialized);

        assertEquals(6, deserialized.getDirectory().size());
        assertEquals(100, deserialized.getInt32(1));
        assertEquals(200L, deserialized.getInt64(2));
        assertEquals("from record1", deserialized.getString(3));
        assertEquals("from record2", deserialized.getString(4));
        assertTrue(deserialized.getBoolean(5));
        assertEquals(3.14f, deserialized.getFloat32(6));
    }

    @Test
    @DisplayName("Merge: overlapping fields - first record wins")
    void testMergeOverlappingFields() throws ImprintException {
        var schemaId = new SchemaId(21, 0xdead9876);

        var record1 = ImprintRecord.builder(schemaId)
                .field(1, "first wins")
                .field(2, 100)
                .field(4, true)
                .build();

        var record2 = ImprintRecord.builder(schemaId)
                .field(1, "second loses")  // Overlapping field
                .field(2, 999)             // Overlapping field
                .field(3, "unique to second")
                .field(4, false)           // Overlapping field
                .build();

        var merged = record1.merge(record2);

        assertEquals(4, merged.getDirectory().size());
        assertEquals("first wins", merged.getString(1));     // First record wins
        assertEquals(100, merged.getInt32(2));               // First record wins
        assertEquals("unique to second", merged.getString(3)); // Only in second
        assertTrue(merged.getBoolean(4));                    // First record wins
    }

    @Test
    @DisplayName("Merge: complex data types and nested records")
    void testMergeComplexTypes() throws ImprintException {
        var schemaId = new SchemaId(22, 0xbeef1111);

        // Create nested records for both
        var nested1 = ImprintRecord.builder(new SchemaId(23, 0x22222222))
                .field(100, "nested in record1")
                .build();

        var nested2 = ImprintRecord.builder(new SchemaId(24, 0x33333333))
                .field(200, "nested in record2")
                .build();

        // Create arrays
        var array1 = Arrays.asList(Value.fromString("array1_item1"), Value.fromString("array1_item2"));
        var array2 = Arrays.asList(Value.fromInt32(10), Value.fromInt32(20));

        // Create maps
        var map1 = new HashMap<MapKey, Value>();
        map1.put(MapKey.fromString("map1_key"), Value.fromString("map1_value"));

        var map2 = new HashMap<MapKey, Value>();
        map2.put(MapKey.fromInt32(42), Value.fromBoolean(true));

        var record1 = ImprintRecord.builder(schemaId)
                .field(1, nested1)
                .field(3, Value.fromArray(array1))
                .field(5, Value.fromMap(map1))
                .build();

        var record2 = ImprintRecord.builder(schemaId)
                .field(2, nested2)
                .field(4, Value.fromArray(array2))
                .field(6, Value.fromMap(map2))
                .build();

        var merged = record1.merge(record2);

        assertEquals(6, merged.getDirectory().size());

        // Verify nested records
        var mergedNested1 = merged.getRow(1);
        assertEquals("nested in record1", mergedNested1.getString(100));

        var mergedNested2 = merged.getRow(2);
        assertEquals("nested in record2", mergedNested2.getString(200));

        // Verify arrays
        var mergedArray1 = merged.getArray(3);
        assertEquals(2, mergedArray1.size());
        assertEquals(Value.fromString("array1_item1"), mergedArray1.get(0));

        var mergedArray2 = merged.getArray(4);
        assertEquals(2, mergedArray2.size());
        assertEquals(Value.fromInt32(10), mergedArray2.get(0));

        // Verify maps
        var mergedMap1 = merged.getMap(5);
        assertEquals(Value.fromString("map1_value"), mergedMap1.get(MapKey.fromString("map1_key")));

        var mergedMap2 = merged.getMap(6);
        assertEquals(Value.fromBoolean(true), mergedMap2.get(MapKey.fromInt32(42)));
    }

    @Test
    @DisplayName("Project and Merge: chained operations")
    void testProjectAndMergeChained() throws ImprintException {
        var schemaId = new SchemaId(30, 0xabcdabcd);

        // Create a large record
        var fullRecord = ImprintRecord.builder(schemaId)
                .field(1, "field1")
                .field(2, "field2")
                .field(3, "field3")
                .field(4, "field4")
                .field(5, "field5")
                .field(6, "field6")
                .build();

        // Project different subsets
        var projection1 = fullRecord.project(1, 3, 5);
        var projection2 = fullRecord.project(2, 4, 6);

        assertEquals(3, projection1.getDirectory().size());
        assertEquals(3, projection2.getDirectory().size());

        // Merge the projections back together
        var recomposed = projection1.merge(projection2);

        assertEquals(6, recomposed.getDirectory().size());
        assertEquals("field1", recomposed.getString(1));
        assertEquals("field2", recomposed.getString(2));
        assertEquals("field3", recomposed.getString(3));
        assertEquals("field4", recomposed.getString(4));
        assertEquals("field5", recomposed.getString(5));
        assertEquals("field6", recomposed.getString(6));

        // Test another chain: project the merged result
        var finalProjection = recomposed.project(2, 4, 6);
        assertEquals(3, finalProjection.getDirectory().size());
        assertEquals("field2", finalProjection.getString(2));
        assertEquals("field4", finalProjection.getString(4));
        assertEquals("field6", finalProjection.getString(6));
    }

    @Test
    @DisplayName("Merge and Project: empty record handling")
    void testMergeAndProjectEmptyRecords() throws ImprintException {
        var schemaId = new SchemaId(40, 0xeeeeeeee);

        var emptyRecord = ImprintRecord.builder(schemaId).build();
        var nonEmptyRecord = ImprintRecord.builder(schemaId)
                .field(1, "not empty")
                .field(2, 42)
                .build();

        // Test merging with empty
        var merged1 = emptyRecord.merge(nonEmptyRecord);
        var merged2 = nonEmptyRecord.merge(emptyRecord);

        assertEquals(2, merged1.getDirectory().size());
        assertEquals(2, merged2.getDirectory().size());
        assertEquals("not empty", merged1.getString(1));
        assertEquals("not empty", merged2.getString(1));

        // Test projecting empty record
        var projectedEmpty = emptyRecord.project(1, 2, 3);
        assertEquals(0, projectedEmpty.getDirectory().size());

        // Test projecting non-existent fields
        var projectedNonExistent = nonEmptyRecord.project(99, 100);
        assertEquals(0, projectedNonExistent.getDirectory().size());
    }

    @Test
    @DisplayName("Project and Merge: Large record operations")
    void testLargeRecordOperations() throws ImprintException {
        var schemaId = new SchemaId(50, 0xffffffff);

        // Create a record with many fields
        var builder = ImprintRecord.builder(schemaId);
        for (int i = 1; i <= 100; i++) {
            builder.field(i, "field_" + i + "_data");
        }
        var largeRecord = builder.build();

        assertEquals(100, largeRecord.getDirectory().size());

        // Project a subset (every 10th field)
        int[] projectionFields = new int[10];
        for (int i = 0; i < 10; i++) {
            projectionFields[i] = (i + 1) * 10; // 10, 20, 30, ..., 100
        }

        var projected = largeRecord.project(projectionFields);
        assertEquals(10, projected.getDirectory().size());

        for (int i = 0; i < 10; i++) {
            int fieldId = (i + 1) * 10;
            assertEquals("field_" + fieldId + "_data", projected.getString(fieldId));
        }

        // Create another large record for merging
        var builder2 = ImprintRecord.builder(schemaId);
        for (int i = 101; i <= 150; i++) {
            builder2.field(i, "additional_field_" + i);
        }
        var additionalRecord = builder2.build();

        // Merge the large records
        var merged = largeRecord.merge(additionalRecord);
        assertEquals(150, merged.getDirectory().size());

        // Verify some values from both records
        assertEquals("field_1_data", merged.getString(1));
        assertEquals("field_50_data", merged.getString(50));
        assertEquals("field_100_data", merged.getString(100));
        assertEquals("additional_field_101", merged.getString(101));
        assertEquals("additional_field_150", merged.getString(150));
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
    void testTypeGettersEmptyCollections() throws ImprintException {
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
    void testTypeGetterExceptionFieldNotFound() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        ImprintException ex = assertThrows(ImprintException.class, () -> record.getInt32(99));
        assertEquals(ErrorType.FIELD_NOT_FOUND, ex.getErrorType());
    }

    @Test
    @DisplayName("Type Getters: Exception for Null Field accessed as primitive")
    void testTypeGetterExceptionNullField() throws ImprintException {
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
    void testTypeGetterExceptionTypeMismatch() throws ImprintException {
        var originalRecord = createTestRecordForGetters();
        var record = serializeAndDeserialize(originalRecord);

        ImprintException ex = assertThrows(ImprintException.class, () -> record.getInt32(6)); // Field 6 is a String
        assertEquals(ErrorType.TYPE_MISMATCH, ex.getErrorType());
    }

    @Test
    @DisplayName("Type Getters: Row (Nested Record)")
    void testTypeGetterRow() throws ImprintException {
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

    @Test
    @DisplayName("Boundary Values: Numeric limits and special floating point values")
    void testNumericBoundaryValues() throws ImprintException {
        var schemaId = new SchemaId(60, 0xB0DA12);
        var record = ImprintRecord.builder(schemaId)
                .field(1, Integer.MAX_VALUE)
                .field(2, Integer.MIN_VALUE)
                .field(3, Long.MAX_VALUE)
                .field(4, Long.MIN_VALUE)
                .field(5, Float.MAX_VALUE)
                .field(6, Float.MIN_VALUE)
                .field(7, Float.NaN)
                .field(8, Float.POSITIVE_INFINITY)
                .field(9, Float.NEGATIVE_INFINITY)
                .field(10, Double.MAX_VALUE)
                .field(11, Double.MIN_VALUE)
                .field(12, Double.NaN)
                .field(13, Double.POSITIVE_INFINITY)
                .field(14, Double.NEGATIVE_INFINITY)
                .field(15, -0.0f)
                .field(16, -0.0)
                .build();

        var deserialized = serializeAndDeserialize(record);

        assertEquals(Integer.MAX_VALUE, deserialized.getInt32(1));
        assertEquals(Integer.MIN_VALUE, deserialized.getInt32(2));
        assertEquals(Long.MAX_VALUE, deserialized.getInt64(3));
        assertEquals(Long.MIN_VALUE, deserialized.getInt64(4));
        assertEquals(Float.MAX_VALUE, deserialized.getFloat32(5));
        assertEquals(Float.MIN_VALUE, deserialized.getFloat32(6));
        assertTrue(Float.isNaN(deserialized.getFloat32(7)));
        assertTrue(Float.isInfinite(deserialized.getFloat32(8)) && deserialized.getFloat32(8) > 0);
        assertTrue(Float.isInfinite(deserialized.getFloat32(9)) && deserialized.getFloat32(9) < 0);
        assertEquals(Double.MAX_VALUE, deserialized.getFloat64(10));
        assertEquals(Double.MIN_VALUE, deserialized.getFloat64(11));
        assertTrue(Double.isNaN(deserialized.getFloat64(12)));
        assertTrue(Double.isInfinite(deserialized.getFloat64(13)) && deserialized.getFloat64(13) > 0);
        assertTrue(Double.isInfinite(deserialized.getFloat64(14)) && deserialized.getFloat64(14) < 0);
        assertEquals(-0.0f, deserialized.getFloat32(15));
        assertEquals(-0.0, deserialized.getFloat64(16));
    }

    @Test
    @DisplayName("Unicode and Special Strings: International character support")
    void testUnicodeAndSpecialStrings() throws ImprintException {
        var schemaId = new SchemaId(61, 0x04100DE);
        var record = ImprintRecord.builder(schemaId)
                .field(1, "") // Empty string
                .field(2, " ") // Single space
                .field(3, "\n\t\r") // Whitespace characters
                .field(4, "Hello, 世界! 🌍🚀") // Unicode: CJK + Emoji
                .field(5, "مرحبا بالعالم") // Arabic (RTL)
                .field(6, "Здравствуй мир") // Cyrillic
                .field(7, "こんにちは世界") // Japanese
                .field(8, "\u0000\u0001\u001F") // Control characters
                .field(9, "A".repeat(10000)) // Large string
                .build();

        var deserialized = serializeAndDeserialize(record);

        assertEquals("", deserialized.getString(1));
        assertEquals(" ", deserialized.getString(2));
        assertEquals("\n\t\r", deserialized.getString(3));
        assertEquals("Hello, 世界! 🌍🚀", deserialized.getString(4));
        assertEquals("مرحبا بالعالم", deserialized.getString(5));
        assertEquals("Здравствуй мир", deserialized.getString(6));
        assertEquals("こんにちは世界", deserialized.getString(7));
        assertEquals("\u0000\u0001\u001F", deserialized.getString(8));
        assertEquals("A".repeat(10000), deserialized.getString(9));
    }

    @Test
    @DisplayName("Deep Nesting: Multiple levels of nested records")
    void testDeepNesting() throws ImprintException {
        // Create 5 levels of nesting
        var level5 = ImprintRecord.builder(new SchemaId(65, 5))
                .field(1, "deepest level")
                .build();

        var level4 = ImprintRecord.builder(new SchemaId(64, 4))
                .field(1, level5)
                .field(2, "level 4")
                .build();

        var level3 = ImprintRecord.builder(new SchemaId(63, 3))
                .field(1, level4)
                .field(2, "level 3")
                .build();

        var level2 = ImprintRecord.builder(new SchemaId(62, 2))
                .field(1, level3)
                .field(2, "level 2")
                .build();

        var level1 = ImprintRecord.builder(new SchemaId(61, 1))
                .field(1, level2)
                .field(2, "level 1")
                .build();

        var deserialized = serializeAndDeserialize(level1);

        // Navigate through all levels
        assertEquals("level 1", deserialized.getString(2));
        var l2 = deserialized.getRow(1);
        assertEquals("level 2", l2.getString(2));
        var l3 = l2.getRow(1);
        assertEquals("level 3", l3.getString(2));
        var l4 = l3.getRow(1);
        assertEquals("level 4", l4.getString(2));
        var l5 = l4.getRow(1);
        assertEquals("deepest level", l5.getString(1));
    }

    @Test
    @DisplayName("Map Key Types: All supported map key types")
    void testMapKeyTypeVariations() throws ImprintException {
        var schemaId = new SchemaId(70, 0xAAB5E75);

        // Create maps with different key types
        var stringKeyMap = new HashMap<MapKey, Value>();
        stringKeyMap.put(MapKey.fromString("string_key"), Value.fromString("string_value"));

        var intKeyMap = new HashMap<MapKey, Value>();
        intKeyMap.put(MapKey.fromInt32(42), Value.fromString("int_value"));

        var longKeyMap = new HashMap<MapKey, Value>();
        longKeyMap.put(MapKey.fromInt64(9876543210L), Value.fromString("long_value"));

        var bytesKeyMap = new HashMap<MapKey, Value>();
        bytesKeyMap.put(MapKey.fromBytes(new byte[]{1, 2, 3}), Value.fromString("bytes_value"));

        var record = ImprintRecord.builder(schemaId)
                .field(1, Value.fromMap(stringKeyMap))
                .field(2, Value.fromMap(intKeyMap))
                .field(3, Value.fromMap(longKeyMap))
                .field(4, Value.fromMap(bytesKeyMap))
                .build();

        var deserialized = serializeAndDeserialize(record);

        // Verify all map key types work correctly
        assertEquals(Value.fromString("string_value"), 
            deserialized.getMap(1).get(MapKey.fromString("string_key")));
        assertEquals(Value.fromString("int_value"), 
            deserialized.getMap(2).get(MapKey.fromInt32(42)));
        assertEquals(Value.fromString("long_value"), 
            deserialized.getMap(3).get(MapKey.fromInt64(9876543210L)));
        assertEquals(Value.fromString("bytes_value"), 
            deserialized.getMap(4).get(MapKey.fromBytes(new byte[]{1, 2, 3})));
    }

    @Test
    @DisplayName("Large Data: Memory efficiency with large payloads")
    void testLargeDataHandling() throws ImprintException {
        var schemaId = new SchemaId(80, 0xB16DA7A);

        // Create large byte arrays
        byte[] largeBytes1 = new byte[100_000]; // 100KB
        byte[] largeBytes2 = new byte[500_000]; // 500KB
        Arrays.fill(largeBytes1, (byte) 0xAA);
        Arrays.fill(largeBytes2, (byte) 0xBB);

        // Create large string
        String largeString = "Large data test: " + "X".repeat(50_000);

        var record = ImprintRecord.builder(schemaId)
                .field(1, largeBytes1)
                .field(2, largeBytes2)
                .field(3, largeString)
                .field(4, "small field")
                .build();

        // Verify large record can be serialized and deserialized
        var deserialized = serializeAndDeserialize(record);

        assertArrayEquals(largeBytes1, deserialized.getBytes(1));
        assertArrayEquals(largeBytes2, deserialized.getBytes(2));
        assertEquals(largeString, deserialized.getString(3));
        assertEquals("small field", deserialized.getString(4));

        // Test projection still works with large data
        var projected = record.project(4);
        assertEquals(1, projected.getDirectory().size());
        assertEquals("small field", projected.getString(4));

        // Verify original large data is excluded from projection
        assertTrue(projected.getSerializedSize() < record.getSerializedSize() / 10);
    }

    @Test
    @DisplayName("Error Handling: Empty data detection")
    void testEmptyDataHandling() {
        // Empty data should throw exception
        assertThrows(Exception.class, () -> ImprintRecord.deserialize(new byte[0]));
        
        // Null data should throw exception
        assertThrows(Exception.class, () -> ImprintRecord.deserialize((byte[]) null));
    }

    @Test
    @DisplayName("Complex Operations: Bytes-to-bytes vs object operations equivalence")
    void testBytesToBytesEquivalence() throws ImprintException {
        var schemaId = new SchemaId(100, 0xB17E5);
        
        var record1 = ImprintRecord.builder(schemaId)
                .field(1, "record1 field1")
                .field(3, 100)
                .field(5, true)
                .build();

        var record2 = ImprintRecord.builder(schemaId)
                .field(2, "record2 field2")
                .field(4, 200L)
                .field(6, 3.14)
                .build();

        // Test merge equivalence
        var objectMerged = record1.merge(record2);
        var bytesMerged = com.imprint.ops.ImprintOperations.mergeBytes(
            record1.serializeToBuffer(), 
            record2.serializeToBuffer()
        );
        var bytesMergedRecord = ImprintRecord.deserialize(bytesMerged);

        assertEquals(objectMerged.getDirectory().size(), bytesMergedRecord.getDirectory().size());
        assertEquals(objectMerged.getString(1), bytesMergedRecord.getString(1));
        assertEquals(objectMerged.getString(2), bytesMergedRecord.getString(2));
        assertEquals(objectMerged.getInt32(3), bytesMergedRecord.getInt32(3));

        // Test project equivalence
        var objectProjected = record1.project(1, 3);
        var bytesProjected = com.imprint.ops.ImprintOperations.projectBytes(
            record1.serializeToBuffer(), 1, 3
        );
        var bytesProjectedRecord = ImprintRecord.deserialize(bytesProjected);

        assertEquals(objectProjected.getDirectory().size(), bytesProjectedRecord.getDirectory().size());
        assertEquals(objectProjected.getString(1), bytesProjectedRecord.getString(1));
        assertEquals(objectProjected.getInt32(3), bytesProjectedRecord.getInt32(3));
    }
}