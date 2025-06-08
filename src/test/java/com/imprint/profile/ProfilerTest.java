package com.imprint.profile;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.types.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;

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
@Disabled("Enable manually for profiling")
public class ProfilerTest {
    
    private static final int ITERATIONS = 1_000_000;
    private static final int RECORD_SIZE = 50;
    
    @Test
    void profileFieldAccess() throws Exception {
        var record = createTestRecord();

        runProfileTest("Field Access", () -> {
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
        });
    }
    
    @Test
    void profileSerialization() throws Exception {
        var schemaId = new SchemaId(1, 0x12345678);

        runProfileTest("Serialization (Standard)", () -> {
            // Create and serialize many records (allocation hotspot)
            for (int i = 0; i < 500_000; i++) {
                var builder = ImprintRecord.builder(schemaId);

                // Add various field types
                builder.field(1, Value.fromInt32(i))
                        .field(2, Value.fromString("test-string-" + i))
                        .field(3, Value.fromFloat64(i * 3.14159))
                        .field(4, Value.fromBytes(("bytes-" + i).getBytes()));

                var record = builder.build();
                var serialized = record.serializeToBuffer(); // Potential hotspot

                // Trigger some deserialization
                if (i % 1000 == 0) {
                    var deserialized = ImprintRecord.deserialize(serialized);
                    deserialized.getValue(2); // String decoding hotspot
                }
            }
        });
    }

    @Test
    void profileLargeObjectSerialization() throws Exception {
        var schemaId = new SchemaId(3, 0xabcdef12);
        var largeRecord = createVeryLargeRecord(); // A single large record to be re-serialized

        runProfileTest("Serialization (Large Object)", () -> {
            // Re-serialize the same large object to focus on serialization logic
            // rather than object creation.
            for (int i = 0; i < 100_000; i++) {
                var serialized = largeRecord.serializeToBuffer(); // Hotspot

                if (i % 1000 == 0) {
                    var deserialized = ImprintRecord.deserialize(serialized);
                    deserialized.getValue(10); // Access a field to ensure it works
                }
            }
        });
    }
    
    @Test
    void profileProjection() throws Exception {
        var record = createLargeRecord();

        runProfileTest("Projection", () -> {
            // Simulate analytical workload - project subset of fields repeatedly
            for (int i = 0; i < 50_000; i++) {
                // Project 10 fields out of 100 (common analytical pattern)
                for (int fieldId = 1; fieldId <= 10; fieldId++) {
                    var value = record.getValue(fieldId);
                    if (value != null) {
                        // Force materialization of string values
                        if (value.getTypeCode() == com.imprint.types.TypeCode.STRING) {
                            if (value instanceof Value.StringBufferValue) {
                                ((Value.StringBufferValue) value).getValue();
                            }
                        }
                    }
                }
            }
        });
    }
    
    @Test
    void profileMemoryAllocation() throws Exception {
        runProfileTest("Memory Allocation", () -> {
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
        }, false); // Disable final time reporting as it's not relevant here
    }
    
    // ========== Test Helpers ==========

    /**
     * A wrapper to run a profiling test with boilerplate for timing and setup.
     * @param testName The name of the test to print.
     * @param testLogic The core logic of the test, passed as a lambda.
     */
    private void runProfileTest(String testName, ThrowingRunnable testLogic) throws Exception {
        runProfileTest(testName, testLogic, true);
    }
    
    private void runProfileTest(String testName, ThrowingRunnable testLogic, boolean reportTime) throws Exception {
        System.out.printf("===== Starting Profiler Test: %s =====%n", testName);
        System.out.println("Attach profiler now...");
        Thread.sleep(3000); // Give time to attach profiler

        System.out.printf("Beginning %s profiling...%n", testName);
        long start = System.nanoTime();

        testLogic.run();

        if (reportTime) {
            long duration = System.nanoTime() - start;
            System.out.printf("===== Completed %s in %.2f ms =====%n%n", testName, duration / 1_000_000.0);
        } else {
            System.out.printf("===== %s profiling complete. Check profiler output. =====%n%n", testName);
        }
    }

    /** A functional interface that allows for exceptions, for use in lambdas. */
    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private ImprintRecord createTestRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0xdeadbeef));
        
        for (int i = 1; i <= RECORD_SIZE; i++) {
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
    
    private ImprintRecord createLargeRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(2, 0xcafebabe));
        
        // Create 100 fields with realistic data
        for (int i = 1; i <= 100; i++) {
            switch (i % 5) {
                case 0:
                    builder.field(i, Value.fromInt32(i));
                    break;
                case 1:
                    builder.field(i, Value.fromString("user-name-" + i + "@example.com"));
                    break;
                case 2:
                    builder.field(i, Value.fromString("Some longer descriptive text for field " + i + " that might represent a comment or description"));
                    break;
                case 3:
                    builder.field(i, Value.fromFloat64(i * 2.718281828));
                    break;
                case 4:
                    builder.field(i, Value.fromBytes(String.format("binary-data-%04d", i).getBytes()));
                    break;
            }
        }
        
        return builder.build();
    }

    private ImprintRecord createVeryLargeRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(3, 0xabcdef12));
        var random = new Random(123);

        // Create 200 fields of varying types and sizes
        for (int i = 1; i <= 200; i++) {
            switch (i % 6) {
                case 0:
                    builder.field(i, i * random.nextInt());
                    break;
                case 1:
                    // Medium string
                    builder.field(i, "user-id-" + UUID.randomUUID().toString());
                    break;
                case 2:
                    // Large string
                    builder.field(i, "This is a much larger text block for field " + i + ". It simulates a user comment, a description, or some other form of semi-structured text data. We repeat a sentence to make it longer. This is a much larger text block for field " + i + ". It simulates a user comment, a description, or some other form of semi-structured text data.");
                    break;
                case 3:
                    builder.field(i, random.nextDouble() * 1000);
                    break;
                case 4:
                    // Small byte array
                    var smallBytes = new byte[32];
                    random.nextBytes(smallBytes);
                    builder.field(i, smallBytes);
                    break;
                case 5:
                    // Large byte array
                    var largeBytes = new byte[1024];
                    random.nextBytes(largeBytes);
                    builder.field(i, largeBytes);
                    break;
            }
        }
        return builder.build();
    }
}