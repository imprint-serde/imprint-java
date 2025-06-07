package com.imprint.core;

import com.imprint.error.ImprintException;
import com.imprint.types.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 1, 5);

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
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 7, 1, 5, 3);

            // Then all requested fields should be present
            assertEquals(4, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
            assertEquals("hello", projected.getString(3));
            assertTrue(projected.getBoolean(5));
            assertArrayEquals(new byte[]{1, 2, 3}, projected.getBytes(7));

            // And directory should maintain sorted order
            List<DirectoryEntry> directory = projected.getDirectory();
            for (int i = 1; i < directory.size(); i++) {
                assertTrue(directory.get(i - 1).getId() < directory.get(i).getId(),
                        "Directory entries should be sorted by field id");
            }
        }

        @Test
        @DisplayName("should handle single field projection")
        void shouldHandleSingleFieldProjection() throws ImprintException {
            // When projecting a single field
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 3);

            // Then only that field should be present
            assertEquals(1, projected.getDirectory().size());
            assertEquals("hello", projected.getString(3));
        }

        @Test
        @DisplayName("should preserve all fields when projecting all")
        void shouldPreserveAllFieldsWhenProjectingAll() throws ImprintException {
            // Given all field IDs from the original record
            int[] allFields = multiFieldRecord.getDirectory().stream()
                    .mapToInt(DirectoryEntry::getId)
                    .toArray();

            // When projecting all fields
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, allFields);

            // Then all fields should be present with matching values
            assertEquals(multiFieldRecord.getDirectory().size(), projected.getDirectory().size());

            for (DirectoryEntry entry : multiFieldRecord.getDirectory()) {
                Value originalValue = multiFieldRecord.getValue(entry.getId());
                Value projectedValue = projected.getValue(entry.getId());
                assertEquals(originalValue, projectedValue,
                        "Field " + entry.getId() + " should have matching value");
            }
        }

        @Test
        @DisplayName("should handle empty projection")
        void shouldHandleEmptyProjection() {
            // When projecting no fields
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord);

            // Then result should be empty but valid
            assertEquals(0, projected.getDirectory().size());
            assertEquals(0, projected.getBuffers().getPayload().remaining());
        }

        @Test
        @DisplayName("should ignore nonexistent fields")
        void shouldIgnoreNonexistentFields() throws ImprintException {
            // When projecting mix of existing and non-existing fields
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 1, 99, 100);

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
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 1, 1, 1);

            // Then field should only appear once
            assertEquals(1, projected.getDirectory().size());
            assertEquals(42, projected.getInt32(1));
        }

        @Test
        @DisplayName("should handle projection from empty record")
        void shouldHandleProjectionFromEmptyRecord() {
            // When projecting any fields from empty record
            ImprintRecord projected = ImprintOperations.project(emptyRecord, 1, 2, 3);

            // Then result should be empty but valid
            assertEquals(0, projected.getDirectory().size());
            assertEquals(0, projected.getBuffers().getPayload().remaining());
        }

        @Test
        @DisplayName("should preserve exact byte representation")
        void shouldPreserveExactByteRepresentation() throws ImprintException {
            // Given a field's original bytes
            byte[] originalBytes = multiFieldRecord.getBytes(7);

            // When projecting that field
            ImprintRecord projected = ImprintOperations.project(multiFieldRecord, 7);

            // Then the byte representation should be exactly preserved
            byte[] projectedBytes = projected.getBytes(7);
            assertArrayEquals(originalBytes, projectedBytes,
                    "Byte representation should be identical");
        }

        @Test
        @DisplayName("should reduce payload size when projecting subset")
        void shouldReducePayloadSizeWhenProjectingSubset() throws ImprintException {
            // Given a record with large and small fields
            ImprintRecord largeRecord = ImprintRecord.builder(testSchema)
                    .field(1, 42)                           // 4 bytes
                    .field(2, "x".repeat(1000))           // ~1000+ bytes
                    .field(3, 123L)                        // 8 bytes
                    .field(4, new byte[500])               // 500+ bytes
                    .build();

            int originalPayloadSize = largeRecord.getBuffers().getPayload().remaining();

            // When projecting only the small fields
            ImprintRecord projected = ImprintOperations.project(largeRecord, 1, 3);

            // Then the payload size should be significantly smaller
            assertTrue(projected.getBuffers().getPayload().remaining() < originalPayloadSize,
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
            ImprintRecord record1 = ImprintRecord.builder(testSchema)
                    .field(1, 42)
                    .field(3, "hello")
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(testSchema)
                    .field(2, true)
                    .field(4, 123L)
                    .build();

            // When merging the records
            ImprintRecord merged = ImprintOperations.merge(record1, record2);

            // Then all fields should be present
            assertEquals(4, merged.getDirectory().size());
            assertEquals(42, merged.getInt32(1));
            assertTrue(merged.getBoolean(2));
            assertEquals("hello", merged.getString(3));
            assertEquals(123L, merged.getInt64(4));

            // And directory should be sorted
            List<DirectoryEntry> directory = merged.getDirectory();
            for (int i = 1; i < directory.size(); i++) {
                assertTrue(directory.get(i - 1).getId() < directory.get(i).getId(),
                        "Directory entries should be sorted by field id");
            }
        }

        @Test
        @DisplayName("should merge records with overlapping fields")
        void shouldMergeRecordsWithOverlappingFields() throws ImprintException {
            // Given two records with overlapping fields
            ImprintRecord record1 = ImprintRecord.builder(testSchema)
                    .field(2, "first")
                    .field(3, 42)
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(testSchema)
                    .field(1, true)
                    .field(2, "second")  // Overlapping field
                    .build();

            // When merging the records
            ImprintRecord merged = ImprintOperations.merge(record1, record2);

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
            SchemaId schema1 = new SchemaId(1, 0xdeadbeef);
            SchemaId schema2 = new SchemaId(1, 0xcafebabe);

            ImprintRecord record1 = ImprintRecord.builder(schema1)
                    .field(1, 42)
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(schema2)
                    .field(2, true)
                    .build();

            // When merging the records
            ImprintRecord merged = ImprintOperations.merge(record1, record2);

            // Then schema ID from first record should be preserved
            assertEquals(schema1, merged.getHeader().getSchemaId());
        }

        @Test
        @DisplayName("should handle merge with empty record")
        void shouldHandleMergeWithEmptyRecord() throws ImprintException {
            // When merging with empty record
            ImprintRecord merged1 = ImprintOperations.merge(multiFieldRecord, emptyRecord);
            ImprintRecord merged2 = ImprintOperations.merge(emptyRecord, multiFieldRecord);

            // Then results should contain all original fields
            assertEquals(multiFieldRecord.getDirectory().size(), merged1.getDirectory().size());
            assertEquals(multiFieldRecord.getDirectory().size(), merged2.getDirectory().size());

            // And values should be preserved
            for (DirectoryEntry entry : multiFieldRecord.getDirectory()) {
                Value originalValue = multiFieldRecord.getValue(entry.getId());
                assertEquals(originalValue, merged1.getValue(entry.getId()));
                assertEquals(originalValue, merged2.getValue(entry.getId()));
            }
        }

        @Test
        @DisplayName("should handle merge of two empty records")
        void shouldHandleMergeOfTwoEmptyRecords() throws ImprintException {
            // When merging two empty records
            ImprintRecord merged = ImprintOperations.merge(emptyRecord, emptyRecord);

            // Then result should be empty but valid
            assertEquals(0, merged.getDirectory().size());
            assertEquals(0, merged.getBuffers().getPayload().remaining());
        }

        @Test
        @DisplayName("should maintain correct payload offsets after merge")
        void shouldMaintainCorrectPayloadOffsetsAfterMerge() throws ImprintException {
            // Given records with different field sizes
            ImprintRecord record1 = ImprintRecord.builder(testSchema)
                    .field(1, 42)              // 4 bytes
                    .field(3, "hello")        // 5+ bytes
                    .build();

            ImprintRecord record2 = ImprintRecord.builder(testSchema)
                    .field(2, true)          // 1 byte
                    .field(4, new byte[]{1, 2, 3, 4, 5}) // 5+ bytes
                    .build();

            // When merging
            ImprintRecord merged = ImprintOperations.merge(record1, record2);

            // Then all fields should be accessible with correct values
            assertEquals(42, merged.getInt32(1));
            assertTrue(merged.getBoolean(2));
            assertEquals("hello", merged.getString(3));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, merged.getBytes(4));

            // And directory offsets should be sequential
            List<DirectoryEntry> directory = merged.getDirectory();
            int expectedOffset = 0;
            for (DirectoryEntry entry : directory) {
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
            ImprintRecord merged = ImprintOperations.merge(record1, record2);

            // Then all 200 fields should be present and accessible
            assertEquals(200, merged.getDirectory().size());

            // Spot check some values
            assertEquals(10, merged.getInt32(1));
            assertEquals(500, merged.getInt32(50));
            assertEquals(1000, merged.getInt32(100));
            assertEquals(1010, merged.getInt32(101));
            assertEquals(1500, merged.getInt32(150));
            assertEquals(2000, merged.getInt32(200));
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandling {

        @Test
        @DisplayName("should handle null record gracefully")
        void shouldHandleNullRecordGracefully() {
            assertThrows(NullPointerException.class, () -> ImprintOperations.project(null, 1, 2, 3));

            assertThrows(NullPointerException.class, () -> ImprintOperations.merge(null, multiFieldRecord));

            assertThrows(NullPointerException.class, () -> ImprintOperations.merge(multiFieldRecord, null));
        }

        @Test
        @DisplayName("should handle null field ids gracefully")
        void shouldHandleNullFieldIdsGracefully() {
            assertThrows(NullPointerException.class, () -> ImprintOperations.project(multiFieldRecord, (int[]) null));
        }
    }
}
