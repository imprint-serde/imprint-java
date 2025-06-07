package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintRecordBuilder;
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
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
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
        var builder = ImprintRecord.builder(source.getHeader().getSchemaId());
        
        for (int fieldId : fieldIds) {
            var value = source.getValue(fieldId);
            if (value != null) {
                builder.field(fieldId, value);
            }
        }
        
        return builder.build();
    }

    private ImprintRecord createSparseRecord() throws Exception {
        return ImprintRecord.builder(new SchemaId(1, 0x12345678))
            .field(1000, Value.fromString("sparse_field_1"))
            .field(5000, Value.fromInt32(42))
            .field(10000, Value.fromFloat64(3.14159))
            .field(15000, Value.fromBoolean(true))
            .field(20000, Value.fromString("sparse_field_5"))
            .build();
    }

    private ImprintRecord createDenseRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(2, 0x87654321));
        
        // Dense record with 100 sequential fields
        for (int i = 1; i <= 100; i++) {
            switch (i % 5) {
                case 0:
                    builder.field(i, Value.fromString("string_field_" + i));
                    break;
                case 1:
                    builder.field(i, Value.fromInt32(i * 10));
                    break;
                case 2:
                    builder.field(i, Value.fromFloat64(i * 1.5));
                    break;
                case 3:
                    builder.field(i, Value.fromBoolean(i % 2 == 0));
                    break;
                case 4:
                    builder.field(i, Value.fromInt64(i * 1000L));
                    break;
            }
        }
        
        return builder.build();
    }

    private ImprintRecord createLargeRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(3, 0xABCDEF12));
        
        // Large record with complex fields (arrays, maps)
        builder.field(1, Value.fromString("Large record with complex data"));
        
        // Add a large array
        var list = new ArrayList<Value>();
        for (int i = 0; i < 200; i++) {
            list.add(Value.fromInt32(i));
        }
        builder.field(2, Value.fromArray(list));
        
        // Add a large map
        var map = new HashMap<MapKey, Value>();
        for (int i = 0; i < 100; i++) {
            map.put(MapKey.fromString("key_" + i), Value.fromString("value_" + i));
        }
        builder.field(3, Value.fromMap(map));
        
        // Add more fields
        for (int i = 4; i <= 50; i++) {
            builder.field(i, Value.fromBytes(new byte[1024])); // 1KB byte arrays
        }
        
        return builder.build();
    }
}