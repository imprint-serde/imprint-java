package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.thrift.ProjectedRecord;
import com.imprint.benchmark.thrift.TestRecord;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class ThriftCompetitor extends AbstractCompetitor {

    private final TSerializer serializer;
    private final TDeserializer deserializer;
    private final TestRecord thriftRecord;

    public ThriftCompetitor() {
        super("Thrift");
        try {
            this.serializer = new TSerializer(new TBinaryProtocol.Factory());
            this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            this.thriftRecord = new TestRecord();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Thrift competitor", e);
        }
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try {
            var record = buildThriftRecord(testRecord);
            this.serializedRecord = serializer.serialize(record);
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
            deserializer.deserialize(record, this.serializedRecord);
            bh.consume(record);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            var projected = new ProjectedRecord();
            projected.setId(this.testData.id);
            projected.setTimestamp(this.testData.timestamp);
            projected.setTags(this.testData.tags.stream().limit(5).collect(Collectors.toList()));
            bh.consume(serializer.serialize(projected));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            var r1 = buildThriftRecord(this.testData);
            var r2 = buildThriftRecord(this.testData2);

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
            deserializer.deserialize(record, this.serializedRecord);
            bh.consume(record.getTimestamp());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
} 