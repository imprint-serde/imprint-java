package com.imprint.benchmark.serializers;

import com.google.flatbuffers.FlatBufferBuilder;
import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.flatbuffers.TestRecord;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;

public class FlatBuffersSerializingBenchmark extends AbstractSerializingBenchmark {

    private ByteBuffer serializedRecord1;
    private ByteBuffer serializedRecord2;

    public FlatBuffersSerializingBenchmark() {
        super("FlatBuffers");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.serializedRecord1 = buildRecord(testRecord);
        this.serializedRecord2 = buildRecord(testRecord2);
    }

    private ByteBuffer buildRecord(DataGenerator.TestRecord pojo) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        int idOffset = builder.createString(pojo.id);
        int[] tagsOffsets = pojo.tags.stream().mapToInt(i -> i).toArray();
        int tagsVectorOffset = TestRecord.createTagsVector(builder, tagsOffsets);

        int[] metadataKeysOffsets = pojo.metadata.keySet().stream().mapToInt(builder::createString).toArray();
        // This is not correct FlatBuffers map creation, it's a placeholder.
        // A proper implementation would require a table for each entry.
        // For this benchmark, we'll just serialize the keys vector.
        int metadataVectorOffset = TestRecord.createMetadataVector(builder, metadataKeysOffsets);

        int dataOffset = TestRecord.createDataVector(builder, pojo.data);

        TestRecord.startTestRecord(builder);
        TestRecord.addId(builder, idOffset);
        TestRecord.addTimestamp(builder, pojo.timestamp);
        TestRecord.addFlags(builder, pojo.flags);
        TestRecord.addActive(builder, pojo.active);
        TestRecord.addValue(builder, pojo.value);
        TestRecord.addData(builder, dataOffset);
        TestRecord.addTags(builder, tagsVectorOffset);
        TestRecord.addMetadata(builder, metadataVectorOffset);

        int recordOffset = TestRecord.endTestRecord(builder);
        builder.finish(recordOffset);

        return builder.dataBuffer();
    }

    @Override
    public void serialize(Blackhole bh) {
        bh.consume(buildRecord(this.testData));
    }

    @Override
    public void deserialize(Blackhole bh) {
        bh.consume(TestRecord.getRootAsTestRecord(serializedRecord1));
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {

        FlatBufferBuilder builder = new FlatBufferBuilder(256);
        var original = TestRecord.getRootAsTestRecord(serializedRecord1);

        int idOffset = builder.createString(original.id());
        
        // Manual sublist
        int[] tagsOffsets = new int[5];
        for (int i = 0; i < 5; i++) {
            tagsOffsets[i] = original.tags(i);
        }
        int tagsVectorOffset = TestRecord.createTagsVector(builder, tagsOffsets);

        TestRecord.startTestRecord(builder);
        TestRecord.addId(builder, idOffset);
        TestRecord.addTimestamp(builder, original.timestamp());
        TestRecord.addTags(builder, tagsVectorOffset);
        int recordOffset = TestRecord.endTestRecord(builder);
        builder.finish(recordOffset);

        bh.consume(builder.dataBuffer());
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        // No direct merge operation. Must read both, build a new one.
        var r1 = TestRecord.getRootAsTestRecord(serializedRecord1);
        var r2 = TestRecord.getRootAsTestRecord(serializedRecord2);
        
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        int idOffset = builder.createString(r1.id());

        // Correctly read and rebuild the tags vector
        // For this benchmark, we'll just take tags from the second record
        int[] tagsArray = new int[r2.tagsLength()];
        for (int i = 0; i < r2.tagsLength(); i++) {
            tagsArray[i] = r2.tags(i);
        }
        int tagsVectorOffset = TestRecord.createTagsVector(builder, tagsArray);

        // Correctly read and rebuild the metadata vector
        // For this benchmark, we'll just take metadata from the second record
        int[] metadataOffsets = new int[r2.metadataLength()];
        for (int i = 0; i < r2.metadataLength(); i++) {
            metadataOffsets[i] = builder.createString(r2.metadata(i));
        }
        int metadataVectorOffset = TestRecord.createMetadataVector(builder, metadataOffsets);


        // Correctly read and rebuild the data vector from r1
        ByteBuffer dataBuffer = r1.dataAsByteBuffer();
        byte[] dataArray = new byte[dataBuffer.remaining()];
        dataBuffer.get(dataArray);
        int dataOffset = TestRecord.createDataVector(builder, dataArray);


        TestRecord.startTestRecord(builder);
        TestRecord.addId(builder, idOffset);
        TestRecord.addTimestamp(builder, System.currentTimeMillis()); // new value
        TestRecord.addFlags(builder, r1.flags());
        TestRecord.addActive(builder, false); // new value
        TestRecord.addValue(builder, r1.value());
        TestRecord.addData(builder, dataOffset);
        TestRecord.addTags(builder, tagsVectorOffset);
        TestRecord.addMetadata(builder, metadataVectorOffset);

        int recordOffset = TestRecord.endTestRecord(builder);
        builder.finish(recordOffset);
        bh.consume(builder.dataBuffer());
    }

    @Override
    public void accessField(Blackhole bh) {
        bh.consume(TestRecord.getRootAsTestRecord(serializedRecord1).timestamp());
    }
} 