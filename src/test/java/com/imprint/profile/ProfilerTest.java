package com.imprint.profile;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.types.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Random;

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
    void profileSerialization() throws Exception {
        System.out.println("Starting serialization profiler test...");
        Thread.sleep(3000);
        
        var schemaId = new SchemaId(1, 0x12345678);

        System.out.println("Beginning serialization profiling...");
        long start = System.nanoTime();
        
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
        
        long duration = System.nanoTime() - start;
        System.out.printf("Completed serialization test in %.2f ms%n", duration / 1_000_000.0);
    }
    
    @Test 
    void profileProjection() throws Exception {
        System.out.println("Starting projection profiler test...");
        Thread.sleep(3000);
        
        var record = createLargeRecord();
        
        System.out.println("Beginning projection profiling...");
        long start = System.nanoTime();
        
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
        
        long duration = System.nanoTime() - start;
        System.out.printf("Completed projection test in %.2f ms%n", duration / 1_000_000.0);
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
}