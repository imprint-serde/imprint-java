package com.imprint.benchmark;

import com.imprint.benchmark.serializers.*;
import com.imprint.benchmark.serializers.SerializingBenchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g", 
    "--illegal-access=permit",
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", 
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"})
public class ComparisonBenchmark {

    private static final List<SerializingBenchmark> FRAMEWORKS = List.of(
            new ImprintSerializingBenchmark(),
            new JacksonSerializingBenchmark(),
            new ProtobufSerializingBenchmark(),
            new FlatBuffersSerializingBenchmark(),
            new AvroSerializingBenchmark(),
            new ThriftSerializingBenchmark(),
            new KryoSerializingBenchmark(),
            new MessagePackSerializingBenchmark(),
            new ChronicleWireSerializingBenchmark());

    @Param({"Imprint", "Jackson-JSON", "Protobuf", "FlatBuffers", "Avro-Generic", "Thrift", "Kryo", "MessagePack", "Chronicle-Wire"})
    public String framework;

    private SerializingBenchmark serializingBenchmark;

    @Setup(Level.Trial)
    public void setup() {
        serializingBenchmark = FRAMEWORKS.stream()
                .filter(c -> c.name().equals(framework))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown framework: " + framework));

        // Create the test data
        DataGenerator.TestRecord testRecord1 = DataGenerator.createTestRecord();
        DataGenerator.TestRecord testRecord2 = DataGenerator.createTestRecord();

        // Setup the framework with the data
        serializingBenchmark.setup(testRecord1, testRecord2);
    }

    @Benchmark
    public void serializeRecord(Blackhole bh) {
        serializingBenchmark.serialize(bh);
    }

    @Benchmark
    public void deserializeRecord(Blackhole bh) {
        serializingBenchmark.deserialize(bh);
    }

    @Benchmark
    public void projectThenSerialize(Blackhole bh) {
        serializingBenchmark.projectAndSerialize(bh);
    }

    @Benchmark
    public void mergeThenSerialize(Blackhole bh) {
        serializingBenchmark.mergeAndSerialize(bh);
    }

    @Benchmark
    public void accessSingleField(Blackhole bh) {
        serializingBenchmark.accessField(bh);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}