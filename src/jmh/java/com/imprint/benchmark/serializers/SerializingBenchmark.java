package com.imprint.benchmark.serializers;

import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Defines the contract for a serialization framework in the benchmark.
 */
public interface SerializingBenchmark {
    String name();
    void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2);
    void serialize(Blackhole bh);
    void deserialize(Blackhole bh);
    void projectAndSerialize(Blackhole bh);
    void mergeAndSerialize(Blackhole bh);
    void accessField(Blackhole bh);
} 