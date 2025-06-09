package com.imprint.profile;

import com.imprint.core.ImprintOperations;
import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.types.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * A test designed for profiling hotspots during development.
 * <p>
 * To use with a profiler:
 * 1. Remove @Disabled annotation
 * 2. Run with JProfiler, VisualVM, or async-profiler:
 *    - JProfiler: Attach to test JVM
 *    - VisualVM: jvisualvm, attach to process
 *    - async-profiler: java -jar async-profiler.jar -d 30 -f profile.html <pid>
 * 3. Look for hotspots in CPU sampling
 * <p>
 * Key areas to examine:
 * - Object allocation (memory profiling)
 * - Method call frequency (CPU sampling)
 * - GC pressure (memory profiling)
 * - String operations and UTF-8 encoding
 * - ByteBuffer operations
 */
//@Disabled("Enable manually for profiling")
public class ProfilerTest {

    private static final int ITERATIONS = 1_000_000;
    private static final int RECORD_SIZE = 50;
    private static final int LARGE_RECORD_SIZE = 200;

    @Test
    void profileFieldAccess() throws Exception {
        System.out.println("Starting profiler test - attach profiler now...");
        Thread.sleep(5000); // Give time to attach profiler

        // Create a representative record
        var record = createTestRecord();

        System.out.println("Beginning field access profiling...");
        long start = System.nanoTime();

        // Simulate real-world access patterns
        Random random = new Random(42);
        int hits = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            // Random field access (hotspot)
            int fieldId = random.nextInt(RECORD_SIZE) + 1;
            var value = record.getValue(fieldId);
            if (value != null) {
                hits++;

                // Trigger string decoding (potential hotspot)
                if (value.getTypeCode() == com.imprint.types.TypeCode.STRING) {
                    if (value instanceof Value.StringBufferValue) {
                        ((Value.StringBufferValue) value).getValue();
                    } else {
                        ((Value.StringValue) value).getValue();
                    }
                }
            }

            // Some raw access (zero-copy path)
            if (i % 10 == 0) {
                record.getRawBytes(fieldId);
            }
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Completed %,d field accesses in %.2f ms (avg: %.1f ns/op, hits: %d)%n",
                ITERATIONS, duration / 1_000_000.0, (double) duration / ITERATIONS, hits);
    }

    @Test
    void profileSmallRecordSerialization() throws Exception {
        profileSerialization("small records", RECORD_SIZE, 100_000);
    }

    @Test
    void profileLargeRecordSerialization() throws Exception {
        profileSerialization("large records", LARGE_RECORD_SIZE, 500_000);
    }

    @Test
    void profileProjectionOperations() throws Exception {
        System.out.println("Starting projection profiler test - attach profiler now...");
        Thread.sleep(3000);

        profileSmallProjections();
        profileLargeProjections();
        profileSelectiveProjections();
        profileProjectionMemoryAllocation();
    }

    /**
     * Profile small projections (select 2-5 fields from 20-field records)
     */
    private void profileSmallProjections() throws Exception {
        System.out.println("\\n--- Small Projections (2-5 fields from 20-field records) ---");

        var sourceRecord = createTestRecord(20);
        int[] projectFields = {1, 5, 10, 15}; // 4 fields
        int iterations = 500_000;

        System.out.printf("Beginning small projection profiling (%,d iterations)...%n", iterations);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            // This is the hotspot we want to profile
            var projected = ImprintOperations.project(sourceRecord, projectFields);

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

    /**
     * Profile large projections (select 50-100 fields from 200-field records)
     */
    private void profileLargeProjections() throws Exception {
        System.out.println("\\n--- Large Projections (50 fields from 200-field records) ---");

        var sourceRecord = createTestRecord(200);
        // Select every 4th field for projection
        int[] projectFields = IntStream.range(0, 50)
                .map(i -> (i * 4) + 1)
                .toArray();
        int iterations = 50_000;

        System.out.printf("Beginning large projection profiling (%,d iterations, %d->%d fields)...%n",
                iterations, 200, projectFields.length);
        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            var projected = ImprintOperations.project(sourceRecord, projectFields);

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

    /**
     * Profile selective projections with different selectivity patterns
     */
    private void profileSelectiveProjections() throws Exception {
        System.out.println("\\n--- Selective Projections (various patterns) ---");

        var sourceRecord = createTestRecord(100);
        Random random = new Random(42);
        int iterations = 100_000;

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
                var projected = ImprintOperations.project(sourceRecord, pattern.fields);

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

    /**
     * Profile memory allocation patterns during projection
     */
    private void profileProjectionMemoryAllocation() throws Exception {
        System.out.println("\\n--- Projection Memory Allocation Profiling ---");
        System.out.println("Watch for allocation hotspots and GC pressure...");

        var sourceRecord = createTestRecord(50);
        int[] projectFields = {1, 5, 10, 15, 20, 25}; // 6 fields

        System.out.println("Beginning projection allocation test...");

        // Create allocation pressure to identify hotspots
        for (int batch = 0; batch < 1000; batch++) {
            for (int i = 0; i < 1000; i++) {
                // This should reveal allocation hotspots in:
                // 1. ArrayList<DirectoryEntry> creation
                // 2. ByteBuffer allocation for new payload
                // 3. FieldRange objects
                // 4. SimpleDirectoryEntry creation
                var projected = ImprintOperations.project(sourceRecord, projectFields);

                // Force some field access to trigger additional allocations
                projected.getValue(1); // String decoding allocation
                projected.getValue(5); // Value wrapper allocation
                projected.getRawBytes(10); // ByteBuffer slicing
            }

            if (batch % 100 == 0) {
                System.out.printf("Allocation batch %d/1000 complete%n", batch);
            }
        }

        System.out.println("Projection allocation test complete");
    }

    /**
     * Profile the component operations within projection to identify bottlenecks
     */
    @Test
    void profileProjectionComponents() throws Exception {
        System.out.println("\\n=== Projection Component Profiling ===");
        Thread.sleep(2000);

        var sourceRecord = createTestRecord(100);
        int[] projectFields = {1, 10, 20, 30, 40, 50};
        int iterations = 100_000;

        // Profile individual components that might be hotspots:

        // 1. Field ID sorting and deduplication
        System.out.println("Profiling field ID sorting...");
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            // This mimics the sorting done in project()
            int[] sorted = Arrays.stream(projectFields).distinct().sorted().toArray();
            blackhole(sorted); // Prevent optimization
        }
        long sortTime = System.nanoTime() - start;
        System.out.printf("Field sorting: %.2f ms (%.1f ns/op)%n",
                sortTime / 1_000_000.0, (double) sortTime / iterations);

        // 2. Directory scanning and range calculation
        System.out.println("Profiling directory scanning...");
        var directory = sourceRecord.getDirectory();
        start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            // Simulate the directory scanning logic
            int foundFields = 0;
            for (var entry : directory) {
                for (int fieldId : projectFields) {
                    if (entry.getId() == fieldId) {
                        foundFields++;
                        break;
                    }
                }
            }
            blackhole(foundFields);
        }
        long scanTime = System.nanoTime() - start;
        System.out.printf("Directory scanning: %.2f ms (%.1f ns/op)%n",
                scanTime / 1_000_000.0, (double) scanTime / iterations);

        // 3. ByteBuffer operations (payload copying)
        System.out.println("Profiling ByteBuffer operations...");
        var payload = sourceRecord.getBuffers().getPayload();
        start = System.nanoTime();
        for (int i = 0; i < iterations / 10; i++) { // Fewer iterations for heavy operation
            // Simulate payload copying
            var newPayload = java.nio.ByteBuffer.allocate(100);
            newPayload.order(java.nio.ByteOrder.LITTLE_ENDIAN);

            // Copy some ranges (like buildPayloadFromRanges does)
            for (int j = 0; j < 6; j++) {
                var slice = payload.duplicate();
                slice.position(j * 10).limit((j + 1) * 10);
                newPayload.put(slice);
            }
            newPayload.flip();
            blackhole(newPayload);
        }
        long bufferTime = System.nanoTime() - start;
        System.out.printf("ByteBuffer operations: %.2f ms (%.1f μs/op)%n",
                bufferTime / 1_000_000.0, (double) bufferTime / (iterations / 10) / 1000.0);
    }

    /**
     * Profile serialization performance with records of a given size.
     * This method abstracts the core serialization profiling logic to work
     * with records of different sizes and complexities.
     */
    private void profileSerialization(String testName, int recordSize, int iterations) throws Exception {
        System.out.printf("Starting %s serialization profiler test...%n", testName);
        Thread.sleep(3000);

        var schemaId = new SchemaId(1, 0x12345678);

        System.out.printf("Beginning %s serialization profiling (%,d iterations, %d fields)...%n",
                testName, iterations, recordSize);
        long start = System.nanoTime();

        // Create and serialize many records (allocation hotspot)
        for (int i = 0; i < iterations; i++) {
            var builder = ImprintRecord.builder(schemaId);

            // Add various field types based on recordSize
            for (int fieldId = 1; fieldId <= recordSize; fieldId++) {
                switch (fieldId % 7) {
                    case 0:
                        builder.field(fieldId, Value.fromInt32(i + fieldId));
                        break;
                    case 1:
                        builder.field(fieldId, Value.fromInt64(i * 1000L + fieldId));
                        break;
                    case 2:
                        builder.field(fieldId, Value.fromString("test-string-" + i + "-" + fieldId));
                        break;
                    case 3:
                        builder.field(fieldId, Value.fromString("longer-descriptive-text-for-field-" + fieldId + "-iteration-" + i));
                        break;
                    case 4:
                        builder.field(fieldId, Value.fromFloat64(i * 3.14159 + fieldId));
                        break;
                    case 5:
                        builder.field(fieldId, Value.fromBytes(("bytes-" + i + "-" + fieldId).getBytes()));
                        break;
                    case 6:
                        builder.field(fieldId, Value.fromBoolean((i + fieldId) % 2 == 0));
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

    @Test
    void profileMemoryAllocation() throws Exception {
        System.out.println("Starting allocation profiler test...");
        Thread.sleep(3000);

        System.out.println("Beginning allocation profiling - watch for GC events...");

        // Force allocation pressure to reveal GC hotspots
        for (int batch = 0; batch < 1000; batch++) {
            for (int i = 0; i < 1000; i++) {
                var schemaId = new SchemaId(batch, i);
                var builder = ImprintRecord.builder(schemaId);

                // Create strings of varying sizes (allocation pressure)
                builder.field(1, Value.fromString("small"))
                        .field(2, Value.fromString("medium-length-string-" + i))
                        .field(3, Value.fromString("very-long-string-that-will-cause-more-allocation-pressure-" + batch + "-" + i))
                        .field(4, Value.fromBytes(new byte[100 + i % 1000])); // Varying byte arrays

                var record = builder.build();

                // Some deserialization to trigger string decoding allocations
                record.getValue(2);
                record.getValue(3);
            }

            if (batch % 100 == 0) {
                System.out.printf("Completed batch %d/1000%n", batch);
            }
        }

        System.out.println("Allocation test complete - check GC logs and memory profiler");
    }

    // Helper methods and classes

    private ImprintRecord createTestRecord() throws Exception {
        return createTestRecord(RECORD_SIZE);
    }

    private ImprintRecord createTestRecord(int recordSize) throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0xdeadbeef));

        for (int i = 1; i <= recordSize; i++) {
            switch (i % 4) {
                case 0:
                    builder.field(i, Value.fromInt32(i * 100));
                    break;
                case 1:
                    builder.field(i, Value.fromString("field-value-" + i));
                    break;
                case 2:
                    builder.field(i, Value.fromFloat64(i * 3.14159));
                    break;
                case 3:
                    builder.field(i, Value.fromBytes(("bytes-" + i).getBytes()));
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

    private void blackhole(Object obj) {
        // Prevent dead code elimination
        if (obj.hashCode() == System.nanoTime()) {
            System.out.println("Never happens");
        }
    }
}