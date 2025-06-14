package com.imprint.profile;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.stream.IntStream;


//@Disabled
public class ProfilerTest {

    private static final int RECORD_SIZE = 50;
    private static final int LARGE_RECORD_SIZE = 200;

    @Test
    @Tag("merge")
    void profileMergeOperations() throws Exception {
        System.out.println("Starting merge profiler test - attach profiler now...");
        Thread.sleep(3000);

        profileSmallMerges();
        profileLargeMerges();
        profileOverlappingMerges();
        profileDisjointMerges();
    }

    /**
     * Profile small merges (20-field records)
     */
    private void profileSmallMerges() throws Exception {
        System.out.println("\\n--- Small Merges (20-field records) ---");

        var record1 = createTestRecord(20);
        var record2 = createTestRecord(20);
        int iterations = 500_000;

        System.out.printf("Beginning small merge profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            // This is the hotspot we want to profile
            var merged = record1.merge(record2);

            // Simulate some usage to prevent dead code elimination
            if (i % 10_000 == 0) {
                merged.getValue(1); // Trigger value decoding
                merged.getRawBytes(5); // Trigger raw access
            }
            merged.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Small merges: %.2f ms (avg: %.1f μs/merge)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    /**
     * Profile large merges (100-field records)
     */
    private void profileLargeMerges() throws Exception {
        System.out.println("\\n--- Large Merges (100-field records) ---");

        var record1 = createTestRecord(100);
        var record2 = createTestRecord(100);
        int iterations = 100_000;

        System.out.printf("Beginning large merge profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var merged = record1.merge(record2);
            merged.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Large merges: %.2f ms (avg: %.1f μs/merge)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    /**
     * Profile overlapping merges (records with many duplicate field IDs)
     */
    private void profileOverlappingMerges() throws Exception {
        System.out.println("\\n--- Overlapping Merges (50%% field overlap) ---");

        var record1 = createTestRecordWithFieldIds(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        var record2 = createTestRecordWithFieldIds(new int[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
        int iterations = 200_000;

        System.out.printf("Beginning overlapping merge profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var merged = record1.merge(record2);
            merged.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Overlapping merges: %.2f ms (avg: %.1f μs/merge)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    /**
     * Profile disjoint merges (no overlapping field IDs)
     */
    private void profileDisjointMerges() throws Exception {
        System.out.println("\\n--- Disjoint Merges (no field overlap) ---");

        // Create records with completely separate field IDs
        var record1 = createTestRecordWithFieldIds(new int[]{1, 3, 5, 7, 9, 11, 13, 15, 17, 19});
        var record2 = createTestRecordWithFieldIds(new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20});
        int iterations = 200_000;

        System.out.printf("Beginning disjoint merge profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var merged = record1.merge(record2);
            merged.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Disjoint merges: %.2f ms (avg: %.1f μs/merge)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    @Test
    @Tag("serialization")
    @Tag("small-records")
    void profileSmallRecordSerialization() throws Exception {
        profileSerialization("small records", RECORD_SIZE, 600_000);
    }

    @Test
    @Tag("serialization")
    @Tag("large-records")
        /*
        It's usually better to change DEFAULT_CAPACITY in ImprintFieldObjectMap to ensure resizing doesn't happen
        unless you specifically want to profile resizing costs (should happen rarely in reality).
        */
    void profileLargeRecordSerialization() throws Exception {
        profileSerialization("large records", LARGE_RECORD_SIZE, 500_000);
    }

    @Test
    @Tag("projection")
    void profileProjectionOperations() throws Exception {
        Thread.sleep(3000);
        profileSmallProjections();
        profileLargeProjections();
        profileSelectiveProjections();
    }

    // Rest of the methods remain the same...
    private void profileSmallProjections() throws Exception {
        System.out.println("\\n--- Small Projections (2-5 fields from 20-field records) ---");

        var sourceRecord = createTestRecord(20);
        int[] projectFields = {1, 5, 10, 15}; // 4 fields
        int iterations = 500_000;

        System.out.printf("Beginning small projection profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            // This is the hotspot we want to profile
            var projected = sourceRecord.project(projectFields);

            // Simulate some usage to prevent dead code elimination
            if (i % 10_000 == 0) {
                projected.getValue(1); // Trigger value decoding
                projected.getRawBytes(5); // Trigger raw access
            }
            projected.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Small projections: %.2f ms (avg: %.1f μs/projection)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    private void profileLargeProjections() throws Exception {
        System.out.println("\\n--- Large Projections (50 fields from 200-field records) ---");

        var sourceRecord = createTestRecord(200);
        // Select every 4th field for projection
        int[] projectFields = IntStream.range(0, 50)
                .map(i -> (i * 4) + 1)
                .toArray();
        int iterations = 200_000;

        System.out.printf("Beginning large projection profiling (%,d iterations, %d->%d fields)...%n",
                iterations, 200, projectFields.length);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var projected = sourceRecord.project(projectFields);

            // Periodically access some fields to simulate real usage
            if (i % 1_000 == 0) {
                projected.getValue(1);
                projected.getValue(25);
                projected.getValue(49);
            }
            projected.serializeToBuffer();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Large projections: %.2f ms (avg: %.1f μs/projection)%n",
                duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    private void profileSelectiveProjections() throws Exception {
        System.out.println("\\n--- Selective Projections (various patterns) ---");

        var sourceRecord = createTestRecord(100);
        Random random = new Random(42);
        int iterations = 200_000;

        // Test different projection patterns
        var patterns = new ProjectionPattern[]{
                new ProjectionPattern("First few fields", new int[]{1, 2, 3, 4, 5}),
                new ProjectionPattern("Last few fields", new int[]{96, 97, 98, 99, 100}),
                new ProjectionPattern("Scattered fields", new int[]{1, 15, 33, 67, 89, 100}),
                new ProjectionPattern("Random fields", generateRandomFields(random, 100, 10))
        };

        for (var pattern : patterns) {
            System.out.printf("Testing pattern: %s (%d fields)%n",
                    pattern.name, pattern.fields.length);

            long start = System.nanoTime();

            for (int i = 0; i < iterations; i++) {
                var projected = sourceRecord.project(pattern.fields);

                // Simulate field access
                if (i % 5_000 == 0) {
                    projected.getValue(pattern.fields[0]);
                }
                projected.serializeToBuffer();
            }

            long duration = System.nanoTime() - start;
            System.out.printf("  %s: %.2f ms (avg: %.1f μs/projection)%n",
                    pattern.name, duration / 1_000_000.0, (double) duration / iterations / 1000.0);
        }
    }

    private void profileSerialization(String testName, int recordSize, int iterations) throws Exception {
        System.out.printf("Starting %s serialization profiler test...%n", testName);
        Thread.sleep(3000);

        var schemaId = new SchemaId(1, 0x12345678);

        System.out.printf("Beginning %s serialization profiling (%,d iterations, %d fields)...%n", testName, iterations, recordSize);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var builder = ImprintRecord.builder(schemaId);

            // Add various field types based on recordSize
            for (int fieldId = 1; fieldId <= recordSize; fieldId++) {
                switch (fieldId % 7) {
                    case 0:
                        builder.field(fieldId, i + fieldId);
                        break;
                    case 1:
                        builder.field(fieldId, i * 1000L + fieldId);
                        break;
                    case 2:
                        builder.field(fieldId, "test-string-" + i + "-" + fieldId);
                        break;
                    case 3:
                        builder.field(fieldId, "longer-descriptive-text-for-field-" + fieldId + "-iteration-" + i);
                        break;
                    case 4:
                        builder.field(fieldId, i * 3.14159 + fieldId);
                        break;
                    case 5:
                        builder.field(fieldId, ("bytes-" + i + "-" + fieldId).getBytes());
                        break;
                    case 6:
                        builder.field(fieldId, (i + fieldId) % 2 == 0);
                        break;
                }
            }

            var record = builder.build();
            var serialized = record.serializeToBuffer();

            // Trigger some deserialization periodically
            if (i % Math.max(1, iterations / 100) == 0) {
                var deserialized = ImprintRecord.deserialize(serialized);
                // Access a few random fields to trigger value decoding
                for (int fieldId = 1; fieldId <= Math.min(5, recordSize); fieldId++) {
                    deserialized.getValue(fieldId); // String decoding hotspot
                }
            }

            // Progress indicator for long-running tests
            if (i > 0 && i % Math.max(1, iterations / 10) == 0) {
                System.out.printf("Completed %,d/%,d iterations (%.1f%%)%n",
                        i, iterations, (double) i / iterations * 100);
            }
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Completed %s serialization test in %.2f ms (avg: %.1f μs/record)%n",
                testName, duration / 1_000_000.0, (double) duration / iterations / 1000.0);
    }

    private ImprintRecord createTestRecord(int recordSize) throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0xdeadbeef));

        for (int i = 1; i <= recordSize; i++) {
            switch (i % 4) {
                case 0:
                    builder.field(i, i * 100);
                    break;
                case 1:
                    builder.field(i, "field-value-" + i);
                    break;
                case 2:
                    builder.field(i, i * 3.14159);
                    break;
                case 3:
                    builder.field(i, ("bytes-" + i).getBytes());
                    break;
            }
        }

        return builder.build();
    }

    private ImprintRecord createTestRecordWithFieldIds(int[] fieldIds) throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0xdeadbeef));
        for (int fieldId : fieldIds) {
            switch (fieldId % 4) {
                case 0:
                    builder.field(fieldId, fieldId * 100);
                    break;
                case 1:
                    builder.field(fieldId, "field-value-" + fieldId);
                    break;
                case 2:
                    builder.field(fieldId, fieldId * 3.14159);
                    break;
                case 3:
                    builder.field(fieldId, ("bytes-" + fieldId).getBytes());
                    break;
            }
        }

        return builder.build();
    }

    private static class ProjectionPattern {
        final String name;
        final int[] fields;

        ProjectionPattern(String name, int[] fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    private int[] generateRandomFields(Random random, int maxField, int count) {
        return random.ints(count, 1, maxField + 1)
                .distinct()
                .sorted()
                .toArray();
    }
}