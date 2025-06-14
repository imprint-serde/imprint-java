package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintRecordBuilder;
import com.imprint.core.SchemaId;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for ImprintRecord serialization and deserialization operations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SerializationBenchmark {

    private ImprintRecord smallRecord;
    private ImprintRecord mediumRecord;
    private ImprintRecord largeRecord;
    
    private ByteBuffer smallRecordBytes;
    private ByteBuffer mediumRecordBytes;
    private ByteBuffer largeRecordBytes;

    @Setup
    public void setup() throws Exception {
        // Create test records of varying sizes for deserialization benchmarks
        smallRecord = createSmallRecord().build();
        mediumRecord = createMediumRecord().build();
        largeRecord = createLargeRecord().build();
        
        // Pre-serialize for deserialization benchmarks
        smallRecordBytes = smallRecord.serializeToBuffer();
        mediumRecordBytes = mediumRecord.serializeToBuffer();
        largeRecordBytes = largeRecord.serializeToBuffer();
    }

    // ===== SERIALIZATION BENCHMARKS =====

    @Benchmark
    public void buildAndSerializeSmallRecord(Blackhole bh) throws Exception {
        ByteBuffer result = createSmallRecord().buildToBuffer();
        bh.consume(result);
    }

    @Benchmark
    public void buildAndSerializeMediumRecord(Blackhole bh) throws Exception {
        ByteBuffer result = createMediumRecord().buildToBuffer();
        bh.consume(result);
    }

    @Benchmark
    public void buildAndSerializeLargeRecord(Blackhole bh) throws Exception {
        ByteBuffer result = createLargeRecord().buildToBuffer();
        bh.consume(result);
    }

    // ===== DESERIALIZATION BENCHMARKS =====

    @Benchmark
    public void deserializeSmallRecord(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(smallRecordBytes.duplicate());
        bh.consume(result);
    }

    @Benchmark
    public void deserializeMediumRecord(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(mediumRecordBytes.duplicate());
        bh.consume(result);
    }

    @Benchmark
    public void deserializeLargeRecord(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(largeRecordBytes.duplicate());
        bh.consume(result);
    }

    // ===== HELPER METHODS =====

    private ImprintRecordBuilder createSmallRecord() throws Exception {
        // Small record: ~10 fields, simple types
        return ImprintRecord.builder(new SchemaId(1, 0x12345678))
            .field(1, "Product")
            .field(2, 12345)
            .field(3, 99.99)
            .field(4, true)
            .field(5, "Electronics");
    }

    private ImprintRecordBuilder createMediumRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0x12345678));
        
        // Medium record: ~50 fields, mixed types including arrays
        builder.field(1, "Product");
        builder.field(2, 12345);
        builder.field(3, 99.99);
        builder.field(4, true);
        builder.field(5, "Electronics");
        
        // Add array field
        var tags = Arrays.asList(
            "popular",
            "trending",
            "bestseller"
        );
        builder.field(6, tags);
        
        // Add map field (all string values for consistency)
        var metadata = new HashMap<String, Object>();
        metadata.put("manufacturer", "TechCorp");
        metadata.put("model", "TC-2024");
        metadata.put("year", "2024");
        builder.field(7, metadata);
        
        // Add more fields for medium size
        for (int i = 8; i <= 50; i++) {
            builder.field(i, "field_" + i + "_value");
        }
        
        return builder;
    }

    private ImprintRecordBuilder createLargeRecord() throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0x12345678));
        
        // Large record: ~200 fields, complex nested structures
        builder.field(1, "LargeProduct");
        builder.field(2, 12345);
        builder.field(3, 99.99);
        
        // Large array
        var largeArray = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            largeArray.add("item_" + i);
        }
        builder.field(4, largeArray);
        
        // Large map
        var largeMap = new HashMap<String, String>();
        for (int i = 0; i < 50; i++) {
            largeMap.put("key_" + i, "value_" + i);
        }
        builder.field(5, largeMap);
        
        // Many string fields
        for (int i = 6; i <= 200; i++) {
            builder.field(i, "this_is_a_longer_field_value_for_field_" + i + "_to_increase_record_size");
        }
        
        return builder;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SerializationBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(5)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.NANOSECONDS)
                .build();

        new Runner(opt).run();
    }
}