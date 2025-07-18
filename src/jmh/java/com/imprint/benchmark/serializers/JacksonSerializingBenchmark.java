package com.imprint.benchmark.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

public class JacksonSerializingBenchmark extends AbstractSerializingBenchmark {

    private final ObjectMapper mapper;
    private byte[] serializedRecord;
    private byte[] serializedRecord2;

    public JacksonSerializingBenchmark() {
        super("Jackson-JSON");
        this.mapper = new ObjectMapper();
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try {
            this.serializedRecord = mapper.writeValueAsBytes(testRecord);
            this.serializedRecord2 = mapper.writeValueAsBytes(testRecord2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(Blackhole bh) {
        try {
            bh.consume(mapper.writeValueAsBytes(this.testData));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            bh.consume(mapper.readValue(serializedRecord, DataGenerator.TestRecord.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            // Full round trip: deserialize, project to a new object, re-serialize
            var original = mapper.readValue(serializedRecord, DataGenerator.TestRecord.class);

            // Simulate by creating the projected object and serializing it
            var projected = new DataGenerator.ProjectedRecord();
            projected.id = original.id;
            projected.timestamp = original.timestamp;
            projected.tags = original.tags.subList(0, 5);
            
            bh.consume(mapper.writeValueAsBytes(projected));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            var r1 = mapper.readValue(serializedRecord, DataGenerator.TestRecord.class);
            var r2 = mapper.readValue(serializedRecord2, DataGenerator.TestRecord.class);
            // Simulate by creating a new merged object and serializing it
            var merged = new DataGenerator.TestRecord();
            merged.id = r1.id;
            merged.timestamp = System.currentTimeMillis(); // new value
            merged.flags = r1.flags;
            merged.active = false; // new value
            merged.value = r1.value;
            merged.data = r1.data;
            merged.tags = r2.tags;
            merged.metadata = r2.metadata;
            
            bh.consume(mapper.writeValueAsBytes(merged));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try {
            var map = mapper.readValue(serializedRecord, java.util.Map.class);
            bh.consume(map.get("timestamp"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 