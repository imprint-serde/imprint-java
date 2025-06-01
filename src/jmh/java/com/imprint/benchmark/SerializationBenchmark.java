package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

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
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
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
        // Create test records of varying sizes
        smallRecord = createSmallRecord();
        mediumRecord = createMediumRecord();
        largeRecord = createLargeRecord();
        
        // Pre-serialize for deserialization benchmarks
        smallRecordBytes = smallRecord.serializeToBuffer();
        mediumRecordBytes = mediumRecord.serializeToBuffer();
        largeRecordBytes = largeRecord.serializeToBuffer();
    }

    // ===== SERIALIZATION BENCHMARKS =====

    @Benchmark
    public void serializeSmallRecord(Blackhole bh) {
        ByteBuffer result = smallRecord.serializeToBuffer();
        bh.consume(result);
    }

    @Benchmark
    public void serializeMediumRecord(Blackhole bh) {
        ByteBuffer result = mediumRecord.serializeToBuffer();
        bh.consume(result);
    }

    @Benchmark
    public void serializeLargeRecord(Blackhole bh) {
        ByteBuffer result = largeRecord.serializeToBuffer();
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

    private ImprintRecord createSmallRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        // Small record: ~10 fields, simple types
        writer.addField(1, Value.fromString("Product"));
        writer.addField(2, Value.fromInt32(12345));
        writer.addField(3, Value.fromFloat64(99.99));
        writer.addField(4, Value.fromBoolean(true));
        writer.addField(5, Value.fromString("Electronics"));
        
        return writer.build();
    }

    private ImprintRecord createMediumRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        // Medium record: ~50 fields, mixed types including arrays
        writer.addField(1, Value.fromString("Product"));
        writer.addField(2, Value.fromInt32(12345));
        writer.addField(3, Value.fromFloat64(99.99));
        writer.addField(4, Value.fromBoolean(true));
        writer.addField(5, Value.fromString("Electronics"));
        
        // Add array field
        var tags = Arrays.asList(
            Value.fromString("popular"),
            Value.fromString("trending"),
            Value.fromString("bestseller")
        );
        writer.addField(6, Value.fromArray(tags));
        
        // Add map field
        var metadata = new HashMap<MapKey, Value>();
        metadata.put(MapKey.fromString("manufacturer"), Value.fromString("TechCorp"));
        metadata.put(MapKey.fromString("model"), Value.fromString("TC-2024"));
        metadata.put(MapKey.fromString("year"), Value.fromInt32(2024));
        writer.addField(7, Value.fromMap(metadata));
        
        // Add more fields for medium size
        for (int i = 8; i <= 50; i++) {
            writer.addField(i, Value.fromString("field_" + i + "_value"));
        }
        
        return writer.build();
    }

    private ImprintRecord createLargeRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        // Large record: ~200 fields, complex nested structures
        writer.addField(1, Value.fromString("LargeProduct"));
        writer.addField(2, Value.fromInt32(12345));
        writer.addField(3, Value.fromFloat64(99.99));
        
        // Large array
        var largeArray = new ArrayList<Value>();
        for (int i = 0; i < 100; i++) {
            largeArray.add(Value.fromString("item_" + i));
        }
        writer.addField(4, Value.fromArray(largeArray));
        
        // Large map
        var largeMap = new HashMap<MapKey, Value>();
        for (int i = 0; i < 50; i++) {
            largeMap.put(MapKey.fromString("key_" + i), Value.fromString("value_" + i));
        }
        writer.addField(5, Value.fromMap(largeMap));
        
        // Many string fields
        for (int i = 6; i <= 200; i++) {
            writer.addField(i, Value.fromString("this_is_a_longer_field_value_for_field_" + i + "_to_increase_record_size"));
        }
        
        return writer.build();
    }
}