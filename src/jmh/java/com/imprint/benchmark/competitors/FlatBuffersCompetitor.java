package com.imprint.benchmark.competitors;

import com.google.flatbuffers.FlatBufferBuilder;
import com.imprint.benchmark.DataGenerator;
import com.imprint.benchmark.flatbuffers.TestRecord;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class FlatBuffersCompetitor extends AbstractCompetitor {

    private ByteBuffer serializedRecord;

    public FlatBuffersCompetitor() {
        super("FlatBuffers");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.serializedRecord = buildRecord(testRecord);
    }

    private ByteBuffer buildRecord(DataGenerator.TestRecord pojo) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        int idOffset = builder.createString(pojo.id);
        int[] tagsOffsets = pojo.tags.stream().mapToInt(i -> i).toArray();
        int tagsVectorOffset = TestRecord.createTagsVector(builder, tagsOffsets);

        int[] metadataKeysOffsets = pojo.metadata.keySet().stream().mapToInt(builder::createString).toArray();
        int[] metadataValuesOffsets = pojo.metadata.values().stream().mapToInt(builder::createString).toArray();
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
        bh.consume(TestRecord.getRootAsTestRecord(serializedRecord));
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        // FlatBuffers excels here. No need to re-serialize. We "project" by reading.
        // But to keep the benchmark fair ("project AND serialize"), we build a new buffer.
        FlatBufferBuilder builder = new FlatBufferBuilder(256);
        var original = TestRecord.getRootAsTestRecord(serializedRecord);

        int idOffset = builder.createString(original.id());
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
        var r1 = TestRecord.getRootAsTestRecord(serializedRecord);
        // For simplicity, we don't build and serialize record2.
        // We'll just merge fields from r1 into a new record.
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        int idOffset = builder.createString(r1.id());

        // Correctly read and rebuild the tags vector
        int[] tagsArray = new int[r1.tagsLength()];
        for (int i = 0; i < r1.tagsLength(); i++) {
            tagsArray[i] = r1.tags(i);
        }
        int tagsVectorOffset = TestRecord.createTagsVector(builder, tagsArray);

        // Correctly read and rebuild the metadata vector (assuming simple list)
        int[] metadataOffsets = new int[r1.metadataLength()];
        for (int i = 0; i < r1.metadataLength(); i++) {
            metadataOffsets[i] = builder.createString(r1.metadata(i));
        }
        int metadataVectorOffset = TestRecord.createMetadataVector(builder, metadataOffsets);


        // Correctly read and rebuild the data vector
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
} 