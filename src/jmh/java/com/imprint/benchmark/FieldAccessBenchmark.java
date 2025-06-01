package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for ImprintRecord field access and projection operations.
 * Tests the zero-copy field access performance claims.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class FieldAccessBenchmark {

    private ImprintRecord sparseRecord;
    private ImprintRecord denseRecord;
    private ImprintRecord largeRecord;
    
    // Field IDs for testing different access patterns
    private int[] firstFields;
    private int[] middleFields;
    private int[] lastFields;
    private int[] randomFields;
    private int[] allFields;

    @Setup
    public void setup() throws Exception {
        sparseRecord = createSparseRecord();     // Few fields, large field IDs
        denseRecord = createDenseRecord();       // Many sequential fields
        largeRecord = createLargeRecord();       // Large record with complex data
        
        // Setup field access patterns
        firstFields = new int[]{1, 2, 3, 4, 5};
        middleFields = new int[]{45, 46, 47, 48, 49};
        lastFields = new int[]{95, 96, 97, 98, 99};
        randomFields = new int[]{7, 23, 41, 67, 89};
        allFields = new int[100];
        for (int i = 0; i < 100; i++) {
            allFields[i] = i + 1;
        }
    }

    // ===== SINGLE FIELD ACCESS BENCHMARKS =====

    @Benchmark
    public void accessFirstField(Blackhole bh) throws Exception {
        var value = denseRecord.getValue(1);
        bh.consume(value);
    }

    @Benchmark
    public void accessMiddleField(Blackhole bh) throws Exception {
        var value = denseRecord.getValue(50);
        bh.consume(value);
    }

    @Benchmark
    public void accessLastField(Blackhole bh) throws Exception {
        var value = denseRecord.getValue(100);
        bh.consume(value);
    }

    @Benchmark
    public void accessNonExistentField(Blackhole bh) throws Exception {
        var value = denseRecord.getValue(999);
        bh.consume(value);
    }

    // ===== MULTIPLE FIELD ACCESS PATTERNS =====

    @Benchmark
    public void accessFirstFields(Blackhole bh) throws Exception {
        for (int fieldId : firstFields) {
            var value = denseRecord.getValue(fieldId);
            bh.consume(value);
        }
    }

    @Benchmark
    public void accessMiddleFields(Blackhole bh) throws Exception {
        for (int fieldId : middleFields) {
            var value = denseRecord.getValue(fieldId);
            bh.consume(value);
        }
    }

    @Benchmark
    public void accessLastFields(Blackhole bh) throws Exception {
        for (int fieldId : lastFields) {
            var value = denseRecord.getValue(fieldId);
            bh.consume(value);
        }
    }

    @Benchmark
    public void accessRandomFields(Blackhole bh) throws Exception {
        for (int fieldId : randomFields) {
            var value = denseRecord.getValue(fieldId);
            bh.consume(value);
        }
    }

    // ===== FIELD PROJECTION BENCHMARKS =====

    @Benchmark
    public void projectSmallSubset(Blackhole bh) throws Exception {
        // Project 5 fields from a 100-field record
        var projection = simulateProject(denseRecord, firstFields);
        bh.consume(projection);
    }

    @Benchmark
    public void projectMediumSubset(Blackhole bh) throws Exception {
        // Project 25 fields from a 100-field record
        int[] fields = Arrays.copyOf(allFields, 25);
        var projection = simulateProject(denseRecord, fields);
        bh.consume(projection);
    }

    @Benchmark
    public void projectLargeSubset(Blackhole bh) throws Exception {
        // Project 75 fields from a 100-field record
        int[] fields = Arrays.copyOf(allFields, 75);
        var projection = simulateProject(denseRecord, fields);
        bh.consume(projection);
    }

    @Benchmark
    public void projectAllFields(Blackhole bh) throws Exception {
        // Project all fields (should be nearly equivalent to full record)
        var projection = simulateProject(denseRecord, allFields);
        bh.consume(projection);
    }

    // ===== RAW BYTES ACCESS BENCHMARKS =====

    @Benchmark
    public void getRawBytesFirstField(Blackhole bh) {
        var rawBytes = denseRecord.getRawBytes(1);
        bh.consume(rawBytes);
    }

    @Benchmark
    public void getRawBytesMiddleField(Blackhole bh) {
        var rawBytes = denseRecord.getRawBytes(50);
        bh.consume(rawBytes);
    }

    @Benchmark
    public void getRawBytesLastField(Blackhole bh) {
        var rawBytes = denseRecord.getRawBytes(100);
        bh.consume(rawBytes);
    }

    // ===== SPARSE VS DENSE ACCESS PATTERNS =====

    @Benchmark
    public void accessSparseRecord(Blackhole bh) throws Exception {
        // Access fields in sparse record (large field IDs, few fields)
        var value1 = sparseRecord.getValue(1000);
        var value2 = sparseRecord.getValue(5000);
        var value3 = sparseRecord.getValue(10000);
        bh.consume(value1);
        bh.consume(value2);
        bh.consume(value3);
    }

    @Benchmark
    public void accessDenseRecord(Blackhole bh) throws Exception {
        // Access fields in dense record (sequential field IDs)
        var value1 = denseRecord.getValue(1);
        var value2 = denseRecord.getValue(2);
        var value3 = denseRecord.getValue(3);
        bh.consume(value1);
        bh.consume(value2);
        bh.consume(value3);
    }

    // ===== HELPER METHODS =====

    /**
     * Simulates field projection by creating a new record with only specified fields.
     * This should be replaced with actual project API when available.
     */
    private ImprintRecord simulateProject(ImprintRecord source, int[] fieldIds) throws Exception {
        var writer = new ImprintWriter(source.getHeader().getSchemaId());
        
        for (int fieldId : fieldIds) {
            var value = source.getValue(fieldId);
            value.ifPresent(value1 -> writer.addField(fieldId, value1));
        }
        
        return writer.build();
    }

    private ImprintRecord createSparseRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        // Sparse record with large field IDs and few fields
        writer.addField(1000, Value.fromString("sparse_field_1"));
        writer.addField(5000, Value.fromInt32(42));
        writer.addField(10000, Value.fromFloat64(3.14159));
        writer.addField(15000, Value.fromBoolean(true));
        writer.addField(20000, Value.fromString("sparse_field_5"));
        
        return writer.build();
    }

    private ImprintRecord createDenseRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(2, 0x87654321));
        
        // Dense record with 100 sequential fields
        for (int i = 1; i <= 100; i++) {
            switch (i % 5) {
                case 0:
                    writer.addField(i, Value.fromString("string_field_" + i));
                    break;
                case 1:
                    writer.addField(i, Value.fromInt32(i * 10));
                    break;
                case 2:
                    writer.addField(i, Value.fromFloat64(i * 1.5));
                    break;
                case 3:
                    writer.addField(i, Value.fromBoolean(i % 2 == 0));
                    break;
                case 4:
                    writer.addField(i, Value.fromInt64(i * 1000L));
                    break;
            }
        }
        
        return writer.build();
    }

    private ImprintRecord createLargeRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(3, 0x11223344));
        
        // Large record with complex data types
        writer.addField(1, Value.fromString("LargeRecord"));
        
        // Large array field
        var largeArray = new ArrayList<Value>();
        for (int i = 0; i < 1000; i++) {
            largeArray.add(Value.fromString("array_item_" + i));
        }
        writer.addField(2, Value.fromArray(largeArray));
        
        // Large map field
        var largeMap = new HashMap<MapKey, Value>();
        for (int i = 0; i < 100; i++) {
            largeMap.put(MapKey.fromString("key_" + i), Value.fromString("map_value_" + i));
        }
        writer.addField(3, Value.fromMap(largeMap));
        
        // Many regular fields
        for (int i = 4; i <= 50; i++) {
            writer.addField(i, Value.fromString("large_record_field_" + i + "_with_substantial_content"));
        }
        
        return writer.build();
    }
}