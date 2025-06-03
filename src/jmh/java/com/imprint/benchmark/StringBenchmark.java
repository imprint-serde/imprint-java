package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class StringBenchmark {
    
    private static final SchemaId SCHEMA_ID = new SchemaId(1, 42);
    
    // Small strings (typical field names, short values)
    private String smallString5;
    private String smallString20;
    private String smallString50;
    
    // Medium strings (typical text content)
    private String mediumString500;
    private String mediumString2K;
    
    // Large strings (document content, JSON payloads)
    private String largeString10K;
    private String largeString100K;
    private String largeString1M;
    
    // Pre-serialized records for deserialization benchmarks
    private byte[] serializedSmall5;
    private byte[] serializedSmall20;
    private byte[] serializedSmall50;
    private byte[] serializedMedium500;
    private byte[] serializedMedium2K;
    private byte[] serializedLarge10K;
    private byte[] serializedLarge100K;
    private byte[] serializedLarge1M;

    private ImprintRecord preDeserializedSmall5;
    private ImprintRecord preDeserializedMedium500;
    private ImprintRecord preDeserializedLarge100K;
    
    @Setup
    public void setup() throws Exception {
        // Generate strings of different sizes
        smallString5 = generateString(5);
        smallString20 = generateString(20);
        smallString50 = generateString(50);
        mediumString500 = generateString(500);
        mediumString2K = generateString(2 * 1024);
        largeString10K = generateString(10 * 1024);
        largeString100K = generateString(100 * 1024);
        largeString1M = generateString(1024 * 1024);
        
        // Pre-serialize records for deserialization benchmarks
        serializedSmall5 = bufferToArray(createStringRecord(smallString5).serializeToBuffer());
        serializedSmall20 = bufferToArray(createStringRecord(smallString20).serializeToBuffer());
        serializedSmall50 = bufferToArray(createStringRecord(smallString50).serializeToBuffer());
        serializedMedium500 = bufferToArray(createStringRecord(mediumString500).serializeToBuffer());
        serializedMedium2K = bufferToArray(createStringRecord(mediumString2K).serializeToBuffer());
        serializedLarge10K = bufferToArray(createStringRecord(largeString10K).serializeToBuffer());
        serializedLarge100K = bufferToArray(createStringRecord(largeString100K).serializeToBuffer());
        serializedLarge1M = bufferToArray(createStringRecord(largeString1M).serializeToBuffer());

        preDeserializedSmall5 = ImprintRecord.deserialize(serializedSmall5);
        preDeserializedMedium500 = ImprintRecord.deserialize(serializedMedium500);
        preDeserializedLarge100K = ImprintRecord.deserialize(serializedLarge100K);
    }
    
    private String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(i % chars.length()));
        }
        return sb.toString();
    }
    
    private ImprintRecord createStringRecord(String value) throws Exception {
        return ImprintRecord.builder(SCHEMA_ID)
            .field(1, value)
            .build();
    }
    
    private String extractString(Value value) {
        if (value instanceof Value.StringValue) {
            return ((Value.StringValue) value).getValue();
        } else if (value instanceof Value.StringBufferValue) {
            return ((Value.StringBufferValue) value).getValue();
        }
        return null;
    }
    
    private byte[] bufferToArray(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.duplicate().get(array);
        return array;
    }
    
    // Serialization benchmarks
    
    @Benchmark
    public ByteBuffer serializeSmallString5() throws Exception {
        return createStringRecord(smallString5).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeSmallString20() throws Exception {
        return createStringRecord(smallString20).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeSmallString50() throws Exception {
        return createStringRecord(smallString50).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeMediumString500() throws Exception {
        return createStringRecord(mediumString500).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeMediumString2K() throws Exception {
        return createStringRecord(mediumString2K).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeLargeString10K() throws Exception {
        return createStringRecord(largeString10K).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeLargeString100K() throws Exception {
        return createStringRecord(largeString100K).serializeToBuffer();
    }
    
    @Benchmark
    public ByteBuffer serializeLargeString1M() throws Exception {
        return createStringRecord(largeString1M).serializeToBuffer();
    }
    
    // Deserialization benchmarks
    
    @Benchmark
    public ImprintRecord deserializeSmallString5() throws Exception {
        return ImprintRecord.deserialize(serializedSmall5);
    }
    
    @Benchmark
    public ImprintRecord deserializeSmallString20() throws Exception {
        return ImprintRecord.deserialize(serializedSmall20);
    }
    
    @Benchmark
    public ImprintRecord deserializeSmallString50() throws Exception {
        return ImprintRecord.deserialize(serializedSmall50);
    }
    
    @Benchmark
    public ImprintRecord deserializeMediumString500() throws Exception {
        return ImprintRecord.deserialize(serializedMedium500);
    }
    
    @Benchmark
    public ImprintRecord deserializeMediumString2K() throws Exception {
        return ImprintRecord.deserialize(serializedMedium2K);
    }
    
    @Benchmark
    public ImprintRecord deserializeLargeString10K() throws Exception {
        return ImprintRecord.deserialize(serializedLarge10K);
    }
    
    @Benchmark
    public ImprintRecord deserializeLargeString100K() throws Exception {
        return ImprintRecord.deserialize(serializedLarge100K);
    }
    
    @Benchmark
    public ImprintRecord deserializeLargeString1M() throws Exception {
        return ImprintRecord.deserialize(serializedLarge1M);
    }
    
    // String access benchmarks
    
    @Benchmark
    public String accessSmallString5() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedSmall5);
        Value value = record.getValue(1);
        return value != null ? extractString(value) : null;
    }
    
    @Benchmark
    public String accessMediumString500() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedMedium500);
        Value value = record.getValue(1);
        return value != null ? extractString(value) : null;
    }
    
    @Benchmark
    public String accessLargeString100K() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedLarge100K);
        Value value = record.getValue(1);
        return value != null ? extractString(value) : null;
    }
    
    // Raw bytes access benchmarks (zero-copy)
    
    @Benchmark
    public ByteBuffer getRawBytesSmallString5() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedSmall5);
        return record.getRawBytes(1);
    }
    
    @Benchmark
    public ByteBuffer getRawBytesMediumString500() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedMedium500);
        return record.getRawBytes(1);
    }
    
    @Benchmark
    public ByteBuffer getRawBytesLargeString100K() throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(serializedLarge100K);
        return record.getRawBytes(1);
    }
    
    // Size measurement benchmarks
    
    @Benchmark
    public int measureSmallString5Size() throws Exception {
        return createStringRecord(smallString5).serializeToBuffer().remaining();
    }
    
    @Benchmark
    public int measureMediumString500Size() throws Exception {
        return createStringRecord(mediumString500).serializeToBuffer().remaining();
    }
    
    @Benchmark
    public int measureLargeString100KSize() throws Exception {
        return createStringRecord(largeString100K).serializeToBuffer().remaining();
    }

    // Pure string access benchmarks (no record deserialization overhead)
    @Benchmark
    public String pureStringAccessSmall5() throws Exception {
        Value value = preDeserializedSmall5.getValue(1);
        return value != null ? extractString(value) : null;
    }

    @Benchmark
    public String pureStringAccessMedium500() throws Exception {
        Value value = preDeserializedMedium500.getValue(1);
        return value != null ? extractString(value) : null;
    }

    @Benchmark
    public String pureStringAccessLarge100K() throws Exception {
        Value value = preDeserializedLarge100K.getValue(1);
        return value != null ? extractString(value) : null;
    }

    // Test cached vs uncached access
    @Benchmark
    public String cachedStringAccessSmall5() throws Exception {
        // Second access should hit cache
        Value value1 = preDeserializedSmall5.getValue(1);
        String result1 = value1 != null ? extractString(value1) : null;
        Value value2 = preDeserializedSmall5.getValue(1);
        return value2 != null ? extractString(value2) : null;
    }
    
    public static void main(String[] args) throws Exception {
        runDeserializationOnly();
    }

    public static void runAll() throws Exception {
        var opt = new OptionsBuilder()
                .include(StringBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
    
    /**
     * Run only string deserialization benchmarks to measure the impact of 
     * ThreadLocal buffer pool optimization and fast/fallback path performance.
     */
    public static void runDeserializationOnly() throws Exception {
        var opt = new OptionsBuilder()
            .include(StringBenchmark.class.getSimpleName() + ".*deserialize.*") // Only deserialize methods
            .forks(0) // Run in same JVM to avoid serialization issues
            .build();
        new Runner(opt).run();
    }
    
    /**
     * Run only pure string access benchmarks (no record deserialization overhead)
     * to isolate string decode performance with ThreadLocal buffer optimization.
     */
    public static void runStringAccessOnly() throws Exception {
        var opt = new OptionsBuilder()
            .include(StringBenchmark.class.getSimpleName() + ".*(pureStringAccess|cachedStringAccess).*") // Only pure string access methods
            .forks(0) // Run in same JVM to avoid serialization issues
            .build();
        new Runner(opt).run();
    }
}