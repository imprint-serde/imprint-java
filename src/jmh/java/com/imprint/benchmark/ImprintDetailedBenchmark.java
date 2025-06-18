package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintRecordBuilder;
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
 * Detailed breakdown of Imprint serialization performance to identify bottlenecks.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 20, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
public class ImprintDetailedBenchmark {

    private DataGenerator.TestRecord testData;
    private ImprintRecordBuilder preBuiltBuilder;
    private ImprintRecord preBuiltRecord;
    private static final SchemaId SCHEMA_ID = new SchemaId(1, 1);

    @Setup(Level.Trial)
    public void setup() {
        testData = DataGenerator.createTestRecord();
        try {
            preBuiltBuilder = buildRecord(testData);
            preBuiltRecord = preBuiltBuilder.build();
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    private ImprintRecordBuilder buildRecord(DataGenerator.TestRecord pojo) {
        var builder = ImprintRecord.builder(SCHEMA_ID, 8); // Pre-size for 8 fields
        builder.field(0, pojo.id);
        builder.field(1, pojo.timestamp);
        builder.field(2, pojo.flags);
        builder.field(3, pojo.active);
        builder.field(4, pojo.value);
        builder.field(5, pojo.data);
        builder.field(6, pojo.tags);
        builder.field(7, pojo.metadata);
        return builder;
    }

    @Benchmark
    public void fieldAddition(Blackhole bh) {
        // Benchmark: POJO → Builder (field addition only)
        try {
            var builder = buildRecord(testData);
            bh.consume(builder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void buildToBuffer(Blackhole bh) {
        // Benchmark: Builder → Bytes (serialization only)
        try {
            bh.consume(preBuiltBuilder.buildToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void serializeToBuffer(Blackhole bh) {
        // Benchmark: Record → Bytes (just buffer copy)
        bh.consume(preBuiltRecord.serializeToBuffer());
    }

    @Benchmark
    public void fullPipeline(Blackhole bh) {
        // Benchmark: POJO → Builder → Bytes (complete pipeline)
        try {
            bh.consume(buildRecord(testData).buildToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ImprintDetailedBenchmark.class.getSimpleName())
                .forks(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.NANOSECONDS)
                .build();

        new Runner(opt).run();
    }
}