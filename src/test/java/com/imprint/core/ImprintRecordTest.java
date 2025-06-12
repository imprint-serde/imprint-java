package com.imprint.core;

import com.imprint.error.ImprintException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ImprintRecord")
class ImprintRecordTest {

    private SchemaId testSchema;
    private ImprintRecord testRecord;
    private ImprintRecord serializedRecord;

    @BeforeEach
    void setUp() throws ImprintException {
        testSchema = new SchemaId(1, 0x12345678);
        testRecord = ImprintRecord.builder(testSchema)
                .field(1, 42)
                .field(2, "hello")
                .field(3, true)
                .field(4, 3.14159)
                .field(5, new byte[]{1, 2, 3, 4, 5})
                .build();
        serializedRecord = testRecord;
    }

    @Nested
    @DisplayName("Creation")
    class Creation {

        @Test
        @DisplayName("should create from ImprintRecord")
        void shouldCreateFromImprintRecord() {
            var serialized = testRecord;
            
            assertNotNull(serialized);
            assertEquals(testRecord.getDirectory().size(), serialized.getFieldCount());
            assertEquals(testSchema, serialized.getSchemaId());
        }

        @Test
        @DisplayName("should create from serialized bytes")
        void shouldCreateFromSerializedBytes() throws ImprintException {
            var bytes = testRecord.serializeToBuffer();
            var serialized = ImprintRecord.fromBytes(bytes);
            
            assertNotNull(serialized);
            assertEquals(testRecord.getDirectory().size(), serialized.getFieldCount());
            assertEquals(testSchema, serialized.getSchemaId());
        }

        @Test
        @DisplayName("should reject null bytes")
        void shouldRejectNullBytes() {
            assertThrows(NullPointerException.class, () -> ImprintRecord.fromBytes(null));
        }
    }

    @Nested
    @DisplayName("Field Access")
    class FieldAccess {

        @Test
        @DisplayName("should access fields with correct types")
        void shouldAccessFieldsWithCorrectTypes() throws ImprintException {
            assertEquals(Integer.valueOf(42), serializedRecord.getInt32(1));
            assertEquals("hello", serializedRecord.getString(2));
            assertEquals(Boolean.TRUE, serializedRecord.getBoolean(3));
            assertEquals(Double.valueOf(3.14159), serializedRecord.getFloat64(4));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, serializedRecord.getBytes(5));
        }

        @Test
        @DisplayName("should handle non-existent fields correctly")
        void shouldHandleNonExistentFields() throws ImprintException {
            // getValue should return null for non-existent fields
            assertNull(serializedRecord.getValue(99));
            
            // Typed getters should throw exceptions for non-existent fields
            assertThrows(ImprintException.class, () -> serializedRecord.getString(99));
            assertThrows(ImprintException.class, () -> serializedRecord.getInt32(100));
            
            // hasField should return false
            assertFalse(serializedRecord.hasField(99));
        }

        @Test
        @DisplayName("should check field existence efficiently")
        void shouldCheckFieldExistenceEfficiently() {
            assertTrue(serializedRecord.hasField(1));
            assertTrue(serializedRecord.hasField(2));
            assertTrue(serializedRecord.hasField(3));
            assertFalse(serializedRecord.hasField(99));
        }

        @Test
        @DisplayName("should return correct field count")
        void shouldReturnCorrectFieldCount() {
            assertEquals(5, serializedRecord.getFieldCount());
        }
    }

    @Nested
    @DisplayName("Zero-Copy Operations")
    class ZeroCopyOperations {

        @Test
        @DisplayName("should merge with another ImprintRecord")
        void shouldMergeWithAnotherImprintRecord() throws ImprintException {
            // Create another record
            var otherRecord = ImprintRecord.builder(testSchema)
                    .field(6, "additional")
                    .field(7, 999L)
                    .build();

            // Merge
            var merged = serializedRecord.merge(otherRecord);

            // Verify merged result
            assertEquals(7, merged.getFieldCount());
            assertEquals(Integer.valueOf(42), merged.getInt32(1));
            assertEquals("hello", merged.getString(2));
            assertEquals("additional", merged.getString(6));
            assertEquals(Long.valueOf(999L), merged.getInt64(7));
        }

        @Test
        @DisplayName("should project subset of fields")
        void shouldProjectSubsetOfFields() throws ImprintException {
            var projected = serializedRecord.project(1, 3, 5);

            assertEquals(3, projected.getFieldCount());
            assertEquals(Integer.valueOf(42), projected.getInt32(1));
            assertEquals(Boolean.TRUE, projected.getBoolean(3));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, projected.getBytes(5));
            
            // Should not have other fields
            assertFalse(projected.hasField(2));
            assertFalse(projected.hasField(4));
        }

        @Test
        @DisplayName("should chain project and merge operations")
        void shouldChainProjectAndMergeOperations() throws ImprintException {
            // Create another record
            var otherSerialized = ImprintRecord.builder(testSchema)
                    .field(10, "chained")
                    .build();

            // Chain operations: project this record, then merge with other
            var result = serializedRecord.projectAndMerge(otherSerialized, 1, 2);

            // Should have projected fields plus other record
            assertEquals(3, result.getFieldCount());
            assertEquals(Integer.valueOf(42), result.getInt32(1));
            assertEquals("hello", result.getString(2));
            assertEquals("chained", result.getString(10));
            
            // Should not have non-projected fields
            assertFalse(result.hasField(3));
            assertFalse(result.hasField(4));
            assertFalse(result.hasField(5));
        }
    }

    @Nested
    @DisplayName("Conversion")
    class Conversion {

        @Test
        @DisplayName("should serialize and deserialize consistently")
        void shouldSerializeAndDeserializeConsistently() throws ImprintException {
            var serializedBytes = serializedRecord.serializeToBuffer();
            var deserialized = ImprintRecord.fromBytes(serializedBytes);

            assertEquals(testRecord.getDirectory().size(), deserialized.getDirectory().size());
            assertEquals(testRecord.getInt32(1), deserialized.getInt32(1));
            assertEquals(testRecord.getString(2), deserialized.getString(2));
            assertEquals(testRecord.getBoolean(3), deserialized.getBoolean(3));
        }

        @Test
        @DisplayName("should preserve serialized bytes")
        void shouldPreserveSerializedBytes() {
            var originalBytes = testRecord.serializeToBuffer();
            var preservedBytes = serializedRecord.getSerializedBytes();

            assertEquals(originalBytes.remaining(), preservedBytes.remaining());
            
            // Compare byte content
            var original = originalBytes.duplicate();
            var preserved = preservedBytes.duplicate();
            
            while (original.hasRemaining() && preserved.hasRemaining()) {
                assertEquals(original.get(), preserved.get());
            }
        }
    }

    @Nested
    @DisplayName("Performance Characteristics")
    class PerformanceCharacteristics {

        @Test
        @DisplayName("should have minimal memory footprint")
        void shouldHaveMinimalMemoryFootprint() {
            var originalSize = testRecord.serializeToBuffer().remaining();
            var serializedSize = serializedRecord.getSerializedSize();

            assertEquals(originalSize, serializedSize);
            
            // ImprintRecord should not significantly increase memory usage
            // (just the wrapper object itself)
            assertTrue(serializedSize > 0);
        }

        @Test
        @DisplayName("should support repeated operations efficiently")
        void shouldSupportRepeatedOperationsEfficiently() throws ImprintException {
            // Multiple field access should not cause performance degradation
            for (int i = 0; i < 100; i++) {
                assertEquals(Integer.valueOf(42), serializedRecord.getInt32(1));
                assertEquals("hello", serializedRecord.getString(2));
                assertTrue(serializedRecord.hasField(3));
            }
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("should handle empty projection")
        void shouldHandleEmptyProjection() throws ImprintException {
            var projected = serializedRecord.project();
            assertEquals(0, projected.getFieldCount());
        }

        @Test
        @DisplayName("should handle projection with non-existent fields")
        void shouldHandleProjectionWithNonExistentFields() throws ImprintException {
            var projected = serializedRecord.project(1, 99, 100);
            assertEquals(1, projected.getFieldCount());
            assertEquals(Integer.valueOf(42), projected.getInt32(1));
            assertFalse(projected.hasField(99));
            assertFalse(projected.hasField(100));
        }

        @Test
        @DisplayName("should handle merge with empty record")
        void shouldHandleMergeWithEmptyRecord() throws ImprintException {
            var emptySerialized = ImprintRecord.builder(testSchema).build();

            var merged = serializedRecord.merge(emptySerialized);
            assertEquals(serializedRecord.getFieldCount(), merged.getFieldCount());
            assertEquals(Integer.valueOf(42), merged.getInt32(1));
        }
    }

    @Nested
    @DisplayName("Equality and Hashing")
    class EqualityAndHashing {

        @Test
        @DisplayName("should be equal for same serialized data")
        void shouldBeEqualForSameSerializedData() {
            var other = testRecord;
            
            assertEquals(serializedRecord, other);
            assertEquals(serializedRecord.hashCode(), other.hashCode());
        }

        @Test
        @DisplayName("should not be equal for different data")
        void shouldNotBeEqualForDifferentData() throws ImprintException {
            // Different value
            var differentSerialized = ImprintRecord.builder(testSchema)
                    .field(1, 999) // Different value
                    .build();

            assertNotEquals(serializedRecord, differentSerialized);
        }
    }
}