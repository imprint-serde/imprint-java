package com.imprint.ops;

import com.imprint.core.Directory;
import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.error.ImprintException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ImprintOperations")
class ImprintOperationsTest {

    private SchemaId testSchema;
    private ImprintRecord multiFieldRecord;
    private ImprintRecord emptyRecord;

    @BeforeEach
    void setUp() throws ImprintException {
        testSchema = new SchemaId(1, 0xdeadbeef);
        multiFieldRecord = createTestRecord();
        emptyRecord = createEmptyTestRecord();
    }

    private ImprintRecord createTestRecord() throws ImprintException {
        return ImprintRecord.builder(testSchema)
                .field(1, 42)
                .field(3, "hello")
                .field(5, true)
                .field(7, new byte[]{1, 2, 3})
                .build();
    }

    private ImprintRecord createEmptyTestRecord() throws ImprintException {
        return ImprintRecord.builder(testSchema).build();
    }

    @Nested
    @DisplayName("Project Operations")
    class ProjectOperations {

        @Test
        @DisplayName("should project subset of fields")
        void shouldProjectSubsetOfFields() throws ImprintException {
            // When projecting a subset of fields
            ImprintRecord projected = multiFieldRecord.project(1, 5);

            // Then only the requested fields should be present
            assertEquals(2, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
            assertTrue(projected.getBoolean(5));

            // And non-requested fields should be absent
            assertNull(projected.getValue(3));
            assertNull(projected.getValue(7));
        }

        @Test
        @DisplayName("should maintain field order regardless of input order")
        void shouldMaintainFieldOrderRegardlessOfInputOrder() throws ImprintException {
            // When projecting fields in arbitrary order
            ImprintRecord projected = multiFieldRecord.project(7, 1, 5, 3);

            // Then all requested fields should be present
            assertEquals(4, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
            assertEquals("hello", projected.getString(3));
            assertTrue(projected.getBoolean(5));
            assertArrayEquals(new byte[]{1, 2, 3}, projected.getBytes(7));

            // And directory should maintain sorted order
            List<Directory> directory = projected.getDirectory();
            for (int i = 1; i < directory.size(); i++) {
                assertTrue(directory.get(i - 1).getId() < directory.get(i).getId(),
                        "Directory entries should be sorted by field id");
            }
        }

        @Test
        @DisplayName("should handle single field projection")
        void shouldHandleSingleFieldProjection() throws ImprintException {
            // When projecting a single field
            var projected = multiFieldRecord.project(3);

            // Then only that field should be present
            assertEquals(1, projected.getDirectory().size());
            assertEquals("hello", projected.getString(3));
        }

        @Test
        @DisplayName("should preserve all fields when projecting all")
        void shouldPreserveAllFieldsWhenProjectingAll() throws ImprintException {
            // Given all field IDs from the original record
            int[] allFields = multiFieldRecord.getDirectory().stream()
                    .mapToInt(Directory::getId)
                    .toArray();

            // When projecting all fields
            ImprintRecord projected = multiFieldRecord.project(allFields);

            // Then all fields should be present with matching values
            assertEquals(multiFieldRecord.getDirectory().size(), projected.getDirectory().size());

            for (var entry : multiFieldRecord.getDirectory()) {
                var originalValue = multiFieldRecord.getValue(entry.getId());
                var projectedValue = projected.getValue(entry.getId());
                
                // Handle byte arrays specially since they don't use content equality
                if (originalValue instanceof byte[] && projectedValue instanceof byte[]) {
                    assertArrayEquals((byte[]) originalValue, (byte[]) projectedValue,
                            "Field " + entry.getId() + " byte array should have matching content");
                } else {
                    assertEquals(originalValue, projectedValue,
                            "Field " + entry.getId() + " should have matching value");
                }
            }
        }

        @Test
        @DisplayName("should handle empty projection")
        void shouldHandleEmptyProjection() throws ImprintException {
            // When projecting no fields
            var projected = multiFieldRecord.project();

            // Then result should be empty but valid
            assertEquals(0, projected.getDirectory().size());
            assertEquals(0, projected.getFieldCount());
        }

        @Test
        @DisplayName("should ignore nonexistent fields")
        void shouldIgnoreNonexistentFields() throws ImprintException {
            // When projecting mix of existing and non-existing fields
            var projected = multiFieldRecord.project(1, 99, 100);

            // Then only existing fields should be included
            assertEquals(1, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
            assertNull(projected.getValue(99));
            assertNull(projected.getValue(100));
        }

        @Test
        @DisplayName("should deduplicate requested fields")
        void shouldDeduplicateRequestedFields() throws ImprintException {
            // When projecting the same field multiple times
            var projected = multiFieldRecord.project(1, 1, 1);

            // Then field should only appear once
            assertEquals(1, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
        }

        @Test
        @DisplayName("should handle projection from empty record")
        void shouldHandleProjectionFromEmptyRecord() throws ImprintException {
            // When projecting any fields from empty record
            var projected = emptyRecord.project(1, 2, 3);

            // Then result should be empty but valid
            assertEquals(0, projected.getDirectory().size());
            assertEquals(0, projected.getFieldCount());
        }

        @Test
        @DisplayName("should preserve exact byte representation")
        void shouldPreserveExactByteRepresentation() throws ImprintException {
            // Given a field's original bytes
            byte[] originalBytes = multiFieldRecord.getBytes(7);

            // When projecting that field
            var projected = multiFieldRecord.project(7);

            // Then the byte representation should be exactly preserved
            byte[] projectedBytes = projected.getBytes(7);
            assertArrayEquals(originalBytes, projectedBytes,
                    "Byte representation should be identical");
        }

        @Test
        @DisplayName("should reduce payload size when projecting subset")
        void shouldReducePayloadSizeWhenProjectingSubset() throws ImprintException {
            // Given a record with large and small fields
            var largeRecord = ImprintRecord.builder(testSchema)
                    .field(1, 42)                           // 4 bytes
                    .field(2, "x".repeat(1000))           // ~1000+ bytes
                    .field(3, 123L)                        // 8 bytes
                    .field(4, new byte[500])               // 500+ bytes
                    .build();

            int originalPayloadSize = largeRecord.getSerializedSize();

            // When projecting only the small fields
            var projected = largeRecord.project(1, 3);

            // Then the payload size should be significantly smaller
            assertTrue(projected.getSerializedSize() < originalPayloadSize,
                    "Projected payload should be smaller than original");

            // And the values should still be correct
            assertEquals(42, projected.getInt32(1));
            assertEquals(123L, projected.getInt64(3));
        }
    }

    @Nested
    @DisplayName("Merge Operations")
    class MergeOperations {

        @Test
        @DisplayName("should merge records with distinct fields")
        void shouldMergeRecordsWithDistinctFields() throws ImprintException {
            // Given two records with different fields
            var record1 = ImprintRecord.builder(testSchema)
                    .field(1, 42)
                    .field(3, "hello")
                    .build();

            var record2 = ImprintRecord.builder(testSchema)
                    .field(2, true)
                    .field(4, 123L)
                    .build();

            // When merging the records
            var merged = record1.merge(record2);

            // Then all fields should be present
            assertEquals(4, merged.getDirectory().size());
            assertEquals(42, merged.getInt32(1));
            assertTrue(merged.getBoolean(2));
            assertEquals("hello", merged.getString(3));
            assertEquals(123L, merged.getInt64(4));

            // And directory should be sorted
            List<Directory> directory = merged.getDirectory();
            for (int i = 1; i < directory.size(); i++) {
                assertTrue(directory.get(i - 1).getId() < directory.get(i).getId(),
                        "Directory entries should be sorted by field id");
            }
        }

        @Test
        @DisplayName("should merge records with overlapping fields")
        void shouldMergeRecordsWithOverlappingFields() throws ImprintException {
            // Given two records with overlapping fields
            var record1 = ImprintRecord.builder(testSchema)
                    .field(2, "first")
                    .field(3, 42)
                    .build();

            var record2 = ImprintRecord.builder(testSchema)
                    .field(1, true)
                    .field(2, "second")  // Overlapping field
                    .build();

            // When merging the records
            var merged = record1.merge(record2);

            // Then first record's values should take precedence for duplicates
            assertEquals(3, merged.getDirectory().size());
            assertTrue(merged.getBoolean(1));
            assertEquals("first", merged.getString(2)); // First record wins
            assertEquals(42, merged.getInt32(3));
        }

        @Test
        @DisplayName("should preserve schema id from first record")
        void shouldPreserveSchemaIdFromFirstRecord() throws ImprintException {
            // Given two records with different schema IDs
            var schema1 = new SchemaId(1, 0xdeadbeef);
            var schema2 = new SchemaId(1, 0xcafebabe);

            var record1 = ImprintRecord.builder(schema1)
                    .field(1, 42)
                    .build();

            var record2 = ImprintRecord.builder(schema2)
                    .field(2, true)
                    .build();

            // When merging the records
            var merged = record1.merge(record2);

            // Then schema ID from first record should be preserved
            assertEquals(schema1, merged.getHeader().getSchemaId());
        }

        @Test
        @DisplayName("should handle merge with empty record")
        void shouldHandleMergeWithEmptyRecord() throws ImprintException {
            // When merging with empty record
            var merged1 = multiFieldRecord.merge(emptyRecord);
            var merged2 = emptyRecord.merge(multiFieldRecord);

            // Then results should contain all original fields
            assertEquals(multiFieldRecord.getDirectory().size(), merged1.getDirectory().size());
            assertEquals(multiFieldRecord.getDirectory().size(), merged2.getDirectory().size());

            // And values should be preserved
            for (Directory entry : multiFieldRecord.getDirectory()) {
                var originalValue = multiFieldRecord.getValue(entry.getId());
                var merged1Value = merged1.getValue(entry.getId());
                var merged2Value = merged2.getValue(entry.getId());
                
                // Handle byte arrays specially since they don't use content equality
                if (originalValue instanceof byte[]) {
                    assertArrayEquals((byte[]) originalValue, (byte[]) merged1Value,
                            "Field " + entry.getId() + " should be preserved in merged1");
                    assertArrayEquals((byte[]) originalValue, (byte[]) merged2Value,
                            "Field " + entry.getId() + " should be preserved in merged2");
                } else {
                    assertEquals(originalValue, merged1Value,
                            "Field " + entry.getId() + " should be preserved in merged1");
                    assertEquals(originalValue, merged2Value,
                            "Field " + entry.getId() + " should be preserved in merged2");
                }
            }
        }

        @Test
        @DisplayName("should handle merge of two empty records")
        void shouldHandleMergeOfTwoEmptyRecords() throws ImprintException {
            // When merging two empty records
            var merged = emptyRecord.merge(emptyRecord);

            // Then result should be empty but valid
            assertEquals(0, merged.getDirectory().size());
            assertEquals(0, merged.getFieldCount());
        }

        @Test
        @DisplayName("should maintain correct payload offsets after merge")
        void shouldMaintainCorrectPayloadOffsetsAfterMerge() throws ImprintException {
            // Given records with different field sizes
            var record1 = ImprintRecord.builder(testSchema)
                    .field(1, 42)              // 4 bytes
                    .field(3, "hello")        // 5+ bytes
                    .build();

            var record2 = ImprintRecord.builder(testSchema)
                    .field(2, true)          // 1 byte
                    .field(4, new byte[]{1, 2, 3, 4, 5}) // 5+ bytes
                    .build();

            // When merging
            var merged = record1.merge(record2);

            // Then all fields should be accessible with correct values
            assertEquals(42, merged.getInt32(1));
            assertTrue(merged.getBoolean(2));
            assertEquals("hello", merged.getString(3));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, merged.getBytes(4));

            // And directory offsets should be sequential
            List<Directory> directory = merged.getDirectory();
            int expectedOffset = 0;
            for (Directory entry : directory) {
                assertEquals(expectedOffset, entry.getOffset(),
                        "Field " + entry.getId() + " should have correct offset");

                // Calculate next offset
                var fieldData = merged.getRawBytes(entry.getId());
                assertNotNull(fieldData);
                expectedOffset += fieldData.remaining();
            }
        }

        @Test
        @DisplayName("should handle large records efficiently")
        void shouldHandleLargeRecordsEfficiently() throws ImprintException {
            // Given records with many fields
            var builder1 = ImprintRecord.builder(testSchema);
            var builder2 = ImprintRecord.builder(testSchema);

            // Add 100 fields to each record (no overlap)
            for (int i = 1; i <= 100; i++) {
                builder1.field(i, i * 10);
            }

            for (int i = 101; i <= 200; i++) {
                builder2.field(i, i * 10);
            }

            ImprintRecord record1 = builder1.build();
            ImprintRecord record2 = builder2.build();

            // When merging large records
            ImprintRecord merged = record1.merge(record2);

            // Then all 200 fields should be present and accessible
            assertEquals(200, merged.getDirectory().size());

            // Spot check a bunch of random values just to make sure I guess
            assertEquals(10, merged.getInt32(1));
            assertEquals(500, merged.getInt32(50));
            assertEquals(1000, merged.getInt32(100));
            assertEquals(1010, merged.getInt32(101));
            assertEquals(1500, merged.getInt32(150));
            assertEquals(2000, merged.getInt32(200));
        }
    }

    @Nested
    @DisplayName("Bytes-to-Bytes Operations")
    class BytesToBytesOperations {

        @Test
        @DisplayName("should merge bytes with same result as object merge")
        void shouldMergeBytesWithSameResultAsObjectMerge() throws ImprintException {
            // Given two records with distinct fields
            ImprintRecord record1 = ImprintRecord.builder(testSchema)
                    .field(1, 42)
                    .field(3, "hello")
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(testSchema)
                    .field(2, true)
                    .field(4, 123L)
                    .build();

            // When merging using both approaches
            var objectMerged = record1.merge(record2);
            var record1Bytes = record1.serializeToBuffer();
            var record2Bytes = record2.serializeToBuffer();
            var bytesMerged = ImprintOperations.mergeBytes(record1Bytes, record2Bytes);

            // Then results should be functionally equivalent
            var deserializedBytes = ImprintRecord.deserialize(bytesMerged);

            assertEquals(objectMerged.getDirectory().size(), deserializedBytes.getDirectory().size());
            assertEquals(42, deserializedBytes.getInt32(1));
            assertTrue(deserializedBytes.getBoolean(2));
            assertEquals("hello", deserializedBytes.getString(3));
            assertEquals(123L, deserializedBytes.getInt64(4));
        }

        @Test
        @DisplayName("should handle overlapping fields in byte merge")
        void shouldHandleOverlappingFieldsInByteMerge() throws ImprintException {
            // Given two records with overlapping fields
            ImprintRecord record1 = ImprintRecord.builder(testSchema)
                    .field(1, "first")
                    .field(2, 42)
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(testSchema)
                    .field(1, "second")  // Overlapping field
                    .field(3, true)
                    .build();

            // When merging using bytes
            var record1Bytes = record1.serializeToBuffer();
            var record2Bytes = record2.serializeToBuffer();
            var merged = ImprintOperations.mergeBytes(record1Bytes, record2Bytes);

            // Then first record's values should take precedence
            var result = ImprintRecord.deserialize(merged);
            assertEquals(3, result.getDirectory().size());
            assertEquals("first", result.getString(1)); // First record wins
            assertEquals(42, result.getInt32(2));
            assertTrue(result.getBoolean(3));
        }

        @Test
        @DisplayName("should merge empty records correctly")
        void shouldMergeEmptyRecordsCorrectly() throws ImprintException {
            // Given an empty record and a non-empty record
            var emptyRecord = ImprintRecord.builder(testSchema).build();
            var nonEmptyRecord = ImprintRecord.builder(testSchema)
                    .field(1, "test")
                    .build();

            // When merging using bytes
            var emptyBytes = emptyRecord.serializeToBuffer();
            var nonEmptyBytes = nonEmptyRecord.serializeToBuffer();
            
            var merged1 = ImprintOperations.mergeBytes(emptyBytes, nonEmptyBytes);
            var merged2 = ImprintOperations.mergeBytes(nonEmptyBytes, emptyBytes);

            // Then both should contain the non-empty record's data
            var result1 = ImprintRecord.deserialize(merged1);
            var result2 = ImprintRecord.deserialize(merged2);
            
            assertEquals(1, result1.getDirectory().size());
            assertEquals(1, result2.getDirectory().size());
            assertEquals("test", result1.getString(1));
            assertEquals("test", result2.getString(1));
        }

        @Test
        @DisplayName("should project bytes with same result as object project")
        void shouldProjectBytesWithSameResultAsObjectProject() throws ImprintException {
            // Given a record with multiple fields
            ImprintRecord record = ImprintRecord.builder(testSchema)
                    .field(1, 42)
                    .field(2, "hello")
                    .field(3, true)
                    .field(4, 123L)
                    .field(5, new byte[]{1, 2, 3})
                    .build();

            // When projecting using both approaches
            var objectProjected = record.project(2, 4);

            var recordBytes = record.serializeToBuffer();
            var bytesProjected = ImprintOperations.projectBytes(recordBytes, 2, 4);

            // Then results should be functionally equivalent
            var deserializedBytes = ImprintRecord.deserialize(bytesProjected);

            assertEquals(objectProjected.getDirectory().size(), deserializedBytes.getDirectory().size());
            assertEquals("hello", deserializedBytes.getString(2));
            assertEquals(123L, deserializedBytes.getInt64(4));
            
            // Should not have the other fields
            assertNull(deserializedBytes.getValue(1));
            assertNull(deserializedBytes.getValue(3));
            assertNull(deserializedBytes.getValue(5));
        }

        @Test
        @DisplayName("should handle empty projection in bytes")
        void shouldHandleEmptyProjectionInBytes() throws ImprintException {
            // Given a record with fields
            var record = ImprintRecord.builder(testSchema)
                    .field(1, "test")
                    .build();

            // When projecting no fields
            var recordBytes = record.serializeToBuffer();
            var projected = ImprintOperations.projectBytes(recordBytes);

            // Then result should be empty but valid
            var result = ImprintRecord.deserialize(projected);
            assertEquals(0, result.getDirectory().size());
        }

        @Test
        @DisplayName("should handle nonexistent fields in byte projection")
        void shouldHandleNonexistentFieldsInByteProjection() throws ImprintException {
            // Given a record with some fields
            var record = ImprintRecord.builder(testSchema)
                    .field(1, "exists")
                    .field(3, 42)
                    .build();

            // When projecting mix of existing and non-existing fields
            var recordBytes = record.serializeToBuffer();
            var projected = ImprintOperations.projectBytes(recordBytes, 1, 99, 100);

            // Then only existing fields should be included
            var result = ImprintRecord.deserialize(projected);
            assertEquals(1, result.getDirectory().size());
            assertEquals("exists", result.getString(1));
            assertNull(result.getValue(99));
            assertNull(result.getValue(100));
        }

        @Test
        @DisplayName("should handle null buffers gracefully")
        void shouldHandleNullBuffersGracefully() throws ImprintException {
            var validRecord = ImprintRecord.builder(testSchema)
                    .field(1, "test")
                    .build();
            var validBuffer = validRecord.serializeToBuffer();

            // Test null buffer scenarios
            assertThrows(Exception.class, () -> 
                ImprintOperations.mergeBytes(null, validBuffer));
            assertThrows(Exception.class, () -> 
                ImprintOperations.mergeBytes(validBuffer, null));
            assertThrows(Exception.class, () -> 
                ImprintOperations.projectBytes((ByteBuffer)null, 1, 2, 3));
        }

        @Test
        @DisplayName("should validate buffer format and reject invalid data")
        void shouldValidateBufferFormatAndRejectInvalidData() throws ImprintException {
            var validRecord = ImprintRecord.builder(testSchema)
                    .field(1, "test")
                    .build();
            var validBuffer = validRecord.serializeToBuffer();

            // Test invalid magic byte
            var invalidMagic = ByteBuffer.allocate(20);
            invalidMagic.put((byte) 0x99); // Invalid magic
            invalidMagic.put((byte) 0x01); // Valid version
            invalidMagic.flip();
            
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.mergeBytes(invalidMagic, validBuffer));
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.projectBytes(invalidMagic, 1));

            // Test buffer too small
            var tooSmall = ByteBuffer.allocate(5);
            tooSmall.put(new byte[]{1, 2, 3, 4, 5});
            tooSmall.flip();
            
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.mergeBytes(tooSmall, validBuffer));
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.projectBytes(tooSmall, 1));

            // Test invalid version
            var invalidVersion = ByteBuffer.allocate(20);
            invalidVersion.put((byte) 0x49); // Valid magic
            invalidVersion.put((byte) 0x99); // Invalid version
            invalidVersion.flip();
            
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.mergeBytes(invalidVersion, validBuffer));
            assertThrows(ImprintException.class, () -> 
                ImprintOperations.projectBytes(invalidVersion, 1));
        }

        @Test
        @DisplayName("should handle large records efficiently in bytes operations")
        void shouldHandleLargeRecordsEfficientlyInBytesOperations() throws ImprintException {
            // Create records with many fields
            var builder1 = ImprintRecord.builder(testSchema);
            var builder2 = ImprintRecord.builder(testSchema);
            
            // Add many fields
            for (int i = 1; i <= 50; i++) {
                builder1.field(i, "field_" + i);
            }
            for (int i = 51; i <= 100; i++) {
                builder2.field(i, "field_" + i);
            }
            
            var record1 = builder1.build();
            var record2 = builder2.build();
            
            // Test bytes-to-bytes merge with many fields
            var merged = ImprintOperations.mergeBytes(
                record1.serializeToBuffer(), 
                record2.serializeToBuffer()
            );
            var mergedRecord = ImprintRecord.deserialize(merged);
            
            assertEquals(100, mergedRecord.getDirectory().size());
            assertEquals("field_1", mergedRecord.getString(1));
            assertEquals("field_100", mergedRecord.getString(100));
            
            // Test bytes-to-bytes projection with many fields
            int[] projectFields = {1, 25, 50, 75, 100};
            var projected = ImprintOperations.projectBytes(merged, projectFields);
            var projectedRecord = ImprintRecord.deserialize(projected);
            
            assertEquals(5, projectedRecord.getDirectory().size());
            assertEquals("field_1", projectedRecord.getString(1));
            assertEquals("field_25", projectedRecord.getString(25));
            assertEquals("field_100", projectedRecord.getString(100));
        }

        @Test
        @DisplayName("should preserve field order in bytes operations")
        void shouldPreserveFieldOrderInBytesOperations() throws ImprintException {
            var record = ImprintRecord.builder(testSchema)
                    .field(5, "field5")
                    .field(1, "field1")
                    .field(3, "field3")
                    .field(2, "field2")
                    .field(4, "field4")
                    .build();
            
            // Project in random order
            var projected = ImprintOperations.projectBytes(
                record.serializeToBuffer(), 4, 1, 3, 5, 2
            );
            var projectedRecord = ImprintRecord.deserialize(projected);
            
            // Verify fields are still accessible and directory is sorted
            var directory = projectedRecord.getDirectory();
            assertEquals(5, directory.size());
            
            // Directory should be sorted by field ID
            for (int i = 1; i < directory.size(); i++) {
                assertTrue(directory.get(i - 1).getId() < directory.get(i).getId());
            }
            
            // All fields should be accessible
            assertEquals("field1", projectedRecord.getString(1));
            assertEquals("field2", projectedRecord.getString(2));
            assertEquals("field3", projectedRecord.getString(3));
            assertEquals("field4", projectedRecord.getString(4));
            assertEquals("field5", projectedRecord.getString(5));
        }
    }
}
