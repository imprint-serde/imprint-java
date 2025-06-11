package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A minimal base class for competitors, holding the test data.
 */
public abstract class AbstractSerializingBenchmark implements SerializingBenchmark {

    protected final String name;
    protected DataGenerator.TestRecord testData;
    protected DataGenerator.TestRecord testData2;

    protected AbstractSerializingBenchmark(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        this.testData = testRecord;
        this.testData2 = testRecord2;
    }

    @Override
    public void accessField(Blackhole bh) {
        // Default implementation is a no-op
    }
} 