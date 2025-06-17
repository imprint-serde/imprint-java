package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.error.ImprintException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark specifically for Imprint merge operations.
 * Tests various merge scenarios to establish baseline performance before SIMD optimizations.
 *
 * Run with: ./gradlew jmh -Pjmh.include='ImprintMergeBenchmark'
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 15, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
public class ImprintMergeBenchmark {

    private static final SchemaId SCHEMA_ID = new SchemaId(1, 0xdeadbeef);

    // Small merge scenarios (20 fields each)
    private ImprintRecord smallRecord1;
    private ImprintRecord smallRecord2;

    // Large merge scenarios (100 fields each)
    private ImprintRecord largeRecord1;
    private ImprintRecord largeRecord2;

    // Overlapping merge scenarios (50% field overlap)
    private ImprintRecord overlappingRecord1;
    private ImprintRecord overlappingRecord2;

    // Disjoint merge scenarios (no field overlap)
    private ImprintRecord disjointRecord1;
    private ImprintRecord disjointRecord2;

    // Asymmetric merge scenarios (different sizes)
    private ImprintRecord asymmetricSmallRecord;
    private ImprintRecord asymmetricLargeRecord;

    @Setup(Level.Trial)
    public void setup() throws ImprintException {
        // Small records (20 fields each)
        smallRecord1 = createTestRecord(20, 1);
        smallRecord2 = createTestRecord(20, 1001);

        // Large records (100 fields each)
        largeRecord1 = createTestRecord(100, 2001);
        largeRecord2 = createTestRecord(100, 3001);

        // Overlapping records (50% field overlap)
        overlappingRecord1 = createTestRecordWithFieldIds(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        overlappingRecord2 = createTestRecordWithFieldIds(new int[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});

        // Disjoint records (no field overlap)
        disjointRecord1 = createTestRecordWithFieldIds(new int[]{1, 3, 5, 7, 9, 11, 13, 15, 17, 19});
        disjointRecord2 = createTestRecordWithFieldIds(new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20});

        // Asymmetric records (different sizes)
        asymmetricSmallRecord = createTestRecord(10, 4001);
        asymmetricLargeRecord = createTestRecord(80, 5001);
    }

    // Ultra-fast ImprintBuffer-native benchmarks
    @Benchmark
    public void mergeSmallRecordsUltraFast(Blackhole bh) throws ImprintException {
        // Test ultra-fast ImprintBuffer-native merge
        var buffer1 = com.imprint.util.ImprintBuffer.fromByteBuffer(smallRecord1.serializeToBuffer());
        var buffer2 = com.imprint.util.ImprintBuffer.fromByteBuffer(smallRecord2.serializeToBuffer());
        var merged = com.imprint.ops.ImprintOperations.mergeBytesUltraFast(buffer1, buffer2);
        bh.consume(merged);
    }

    @Benchmark
    public void mergeLargeRecordsUltraFast(Blackhole bh) throws ImprintException {
        // Test ultra-fast merge on large records - maximum SIMD optimization
        var buffer1 = com.imprint.util.ImprintBuffer.fromByteBuffer(largeRecord1.serializeToBuffer());
        var buffer2 = com.imprint.util.ImprintBuffer.fromByteBuffer(largeRecord2.serializeToBuffer());
        var merged = com.imprint.ops.ImprintOperations.mergeBytesUltraFast(buffer1, buffer2);
        bh.consume(merged);
    }


    // Helper methods for creating test data
    private ImprintRecord createTestRecord(int fieldCount, int baseValue) throws ImprintException {
        var builder = ImprintRecord.builder(SCHEMA_ID, fieldCount);

        for (int i = 1; i <= fieldCount; i++) {
            switch (i % 7) {
                case 0:
                    builder.field(i, baseValue + i);
                    break;
                case 1:
                    builder.field(i, (long)(baseValue + i) * 1000L);
                    break;
                case 2:
                    builder.field(i, "test-string-" + baseValue + "-" + i);
                    break;
                case 3:
                    builder.field(i, "longer-descriptive-text-for-field-" + i + "-base-" + baseValue);
                    break;
                case 4:
                    builder.field(i, (baseValue + i) * 3.14159);
                    break;
                case 5:
                    builder.field(i, ("bytes-" + baseValue + "-" + i).getBytes());
                    break;
                case 6:
                    builder.field(i, (baseValue + i) % 2 == 0);
                    break;
            }
        }

        return builder.build();
    }

    private ImprintRecord createTestRecordWithFieldIds(int[] fieldIds) throws ImprintException {
        var builder = ImprintRecord.builder(SCHEMA_ID, fieldIds.length);

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

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ImprintMergeBenchmark.class.getSimpleName())
                .forks(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.NANOSECONDS)
                .warmupIterations(3)
                .measurementIterations(15)
                .build();

        new Runner(opt).run();
    }
}