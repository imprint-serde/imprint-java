package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.proto.TestRecordOuterClass;
import org.openjdk.jmh.infra.Blackhole;

public class ProtobufCompetitor extends AbstractCompetitor {

    private byte[] serializedRecord;

    public ProtobufCompetitor() {
        super("Protobuf");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.serializedRecord = buildRecord(testRecord).toByteArray();
    }

    private TestRecordOuterClass.TestRecord buildRecord(DataGenerator.TestRecord pojo) {
        return TestRecordOuterClass.TestRecord.newBuilder()
                .setId(pojo.id)
                .setTimestamp(pojo.timestamp)
                .setFlags(pojo.flags)
                .setActive(pojo.active)
                .setValue(pojo.value)
                .setData(com.google.protobuf.ByteString.copyFrom(pojo.data))
                .addAllTags(pojo.tags)
                .putAllMetadata(pojo.metadata)
                .build();
    }

    @Override
    public void serialize(Blackhole bh) {
        bh.consume(buildRecord(this.testData).toByteArray());
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            bh.consume(TestRecordOuterClass.TestRecord.parseFrom(serializedRecord));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        // Projection with Protobuf can be done by building a new message with a subset of fields.
        // There isn't a direct "project" operation on a parsed message.
        TestRecordOuterClass.TestRecord projected = TestRecordOuterClass.TestRecord.newBuilder()
                .setId(this.testData.id)
                .setTimestamp(this.testData.timestamp)
                .addAllTags(this.testData.tags.subList(0, 5))
                .build();
        bh.consume(projected.toByteArray());
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        // Protobuf's `mergeFrom` is a natural fit here.
        var record1 = buildRecord(this.testData);
        var record2 = buildRecord(this.testData2);

        var merged = record1.toBuilder().mergeFrom(record2).build();
        bh.consume(merged.toByteArray());
    }
} 