package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.proto.TestRecordOuterClass;
import org.openjdk.jmh.infra.Blackhole;

public class ProtobufSerializingBenchmark extends AbstractSerializingBenchmark {

    private byte[] serializedRecord1;
    private byte[] serializedRecord2;

    public ProtobufSerializingBenchmark() {
        super("Protobuf");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.serializedRecord1 = buildRecord(testRecord).toByteArray();
        this.serializedRecord2 = buildRecord(testRecord2).toByteArray();
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
            bh.consume(TestRecordOuterClass.TestRecord.parseFrom(serializedRecord1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            // Full round trip: deserialize, project to a new object, re-serialize
            var original = TestRecordOuterClass.TestRecord.parseFrom(serializedRecord1);

            TestRecordOuterClass.TestRecord projected = TestRecordOuterClass.TestRecord.newBuilder()
                    .setId(original.getId())
                    .setTimestamp(original.getTimestamp())
                    .addAllTags(original.getTagsList().subList(0, 5))
                    .build();
            bh.consume(projected.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            // Protobuf's `mergeFrom` is a natural fit here.
            var record1 = TestRecordOuterClass.TestRecord.parseFrom(serializedRecord1);
            var record2 = TestRecordOuterClass.TestRecord.parseFrom(serializedRecord2);

            var merged = record1.toBuilder().mergeFrom(record2).build();
            bh.consume(merged.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try {
            bh.consume(TestRecordOuterClass.TestRecord.parseFrom(serializedRecord1).getTimestamp());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 