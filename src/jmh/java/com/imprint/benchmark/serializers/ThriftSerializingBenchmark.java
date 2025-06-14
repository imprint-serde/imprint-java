package com.imprint.benchmark.serializers;

import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.thrift.ProjectedRecord;
import com.imprint.benchmark.thrift.TestRecord;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class ThriftSerializingBenchmark extends AbstractSerializingBenchmark {

    private final TSerializer serializer;
    private final TDeserializer deserializer;
    private byte[] serializedRecord1;
    private byte[] serializedRecord2;

    public ThriftSerializingBenchmark() {
        super("Thrift");
        try {
            this.serializer = new TSerializer(new TBinaryProtocol.Factory());
            this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Thrift competitor", e);
        }
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try {
            var record1 = buildThriftRecord(testRecord);
            this.serializedRecord1 = serializer.serialize(record1);
            var record2 = buildThriftRecord(testRecord2);
            this.serializedRecord2 = serializer.serialize(record2);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private TestRecord buildThriftRecord(DataGenerator.TestRecord pojo) {
        var record = new TestRecord();
        record.setId(pojo.id);
        record.setTimestamp(pojo.timestamp);
        record.setFlags(pojo.flags);
        record.setActive(pojo.active);
        record.setValue(pojo.value);
        record.setData(ByteBuffer.wrap(pojo.data));
        record.setTags(pojo.tags);
        record.setMetadata(pojo.metadata);
        return record;
    }

    @Override
    public void serialize(Blackhole bh) {
        try {
            bh.consume(serializer.serialize(buildThriftRecord(this.testData)));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            var record = new TestRecord();
            deserializer.deserialize(record, this.serializedRecord1);
            bh.consume(record);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            // Full round trip: deserialize, project to a new object, re-serialize
            var original = new TestRecord();
            deserializer.deserialize(original, this.serializedRecord1);

            var projected = new ProjectedRecord();
            projected.setId(original.getId());
            projected.setTimestamp(original.getTimestamp());
            projected.setTags(original.getTags().stream().limit(5).collect(Collectors.toList()));
            bh.consume(serializer.serialize(projected));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            var r1 = new TestRecord();
            deserializer.deserialize(r1, this.serializedRecord1);
            var r2 = new TestRecord();
            deserializer.deserialize(r2, this.serializedRecord2);

            var merged = new TestRecord();
            merged.setId(r1.id);
            merged.setTimestamp(System.currentTimeMillis());
            merged.setFlags(r1.flags | r2.flags);
            merged.setActive(false);
            merged.setValue((r1.value + r2.value) / 2);
            merged.setData(r1.data); // Keep r1's data
            merged.setTags(r1.tags);
            r2.tags.forEach(t -> {
                if (!merged.tags.contains(t)) {
                    merged.tags.add(t);
                }
            });
            merged.setMetadata(r1.metadata);
            r2.metadata.forEach(merged.metadata::putIfAbsent);

            bh.consume(serializer.serialize(merged));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try {
            var record = new TestRecord();
            deserializer.deserialize(record, this.serializedRecord1);
            bh.consume(record.getTimestamp());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
} 