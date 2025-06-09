package com.imprint.benchmark;

import com.imprint.benchmark.competitors.*;
import com.imprint.benchmark.competitors.Competitor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
public class ComparisonBenchmark {

    private static final List<Competitor> COMPETITORS = Arrays.asList(
            new ImprintCompetitor(),
            new JacksonJsonCompetitor(),
            new ProtobufCompetitor(),
            new FlatBuffersCompetitor(),
            new AvroCompetitor(),
            new ThriftCompetitor(),
            new KryoCompetitor(),
            new MessagePackCompetitor()
    );

    @Param({"Imprint", "Jackson-JSON", "Protobuf", "FlatBuffers", "Avro-Generic", "Thrift", "Kryo", "MessagePack"})
    public String competitorName;

    private Competitor competitor;

    @Setup(Level.Trial)
    public void setup() {
        // Find the competitor implementation
        competitor = COMPETITORS.stream()
                .filter(c -> c.name().equals(competitorName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown competitor: " + competitorName));

        // Create the test data
        DataGenerator.TestRecord testRecord1 = DataGenerator.createTestRecord();
        DataGenerator.TestRecord testRecord2 = DataGenerator.createTestRecord();

        // Setup the competitor with the data
        competitor.setup(testRecord1, testRecord2);
    }

    @Benchmark
    public void serialize(Blackhole bh) {
        competitor.serialize(bh);
    }

    @Benchmark
    public void deserialize(Blackhole bh) {
        competitor.deserialize(bh);
    }

    @Benchmark
    public void projectAndSerialize(Blackhole bh) {
        competitor.projectAndSerialize(bh);
    }

    @Benchmark
    public void mergeAndSerialize(Blackhole bh) {
        competitor.mergeAndSerialize(bh);
    }

    @Benchmark
    public void accessField(Blackhole bh) {
        competitor.accessField(bh);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}