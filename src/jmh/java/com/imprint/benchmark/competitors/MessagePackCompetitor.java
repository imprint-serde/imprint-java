package com.imprint.benchmark.competitors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imprint.benchmark.DataGenerator;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.openjdk.jmh.infra.Blackhole;

public class MessagePackCompetitor extends AbstractCompetitor {

    private final ObjectMapper mapper;
    private byte[] serializedRecord;
    private byte[] serializedRecord2;

    public MessagePackCompetitor() {
        super("MessagePack");
        this.mapper = new ObjectMapper(new MessagePackFactory());
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
        var projected = new DataGenerator.ProjectedRecord();
        projected.id = this.testData.id;
        projected.timestamp = this.testData.timestamp;
        projected.tags = this.testData.tags.subList(0, 5);
        try {
            bh.consume(mapper.writeValueAsBytes(projected));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        var merged = new DataGenerator.TestRecord();
        merged.id = this.testData.id;
        merged.timestamp = System.currentTimeMillis();
        merged.flags = this.testData.flags;
        merged.active = false;
        merged.value = this.testData.value;
        merged.data = this.testData.data;
        merged.tags = this.testData2.tags;
        merged.metadata = this.testData2.metadata;
        try {
            bh.consume(mapper.writeValueAsBytes(merged));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 