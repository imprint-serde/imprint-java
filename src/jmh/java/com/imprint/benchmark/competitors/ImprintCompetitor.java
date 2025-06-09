package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import com.imprint.core.ImprintOperations;
import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;

public class ImprintCompetitor extends AbstractCompetitor {

    private ImprintRecord record;
    private ImprintRecord record2;
    private ByteBuffer serializedRecord;
    private static final SchemaId SCHEMA_ID = new SchemaId(1, 1);

    public ImprintCompetitor() {
        super("Imprint");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.record = buildRecord(testRecord);
        this.record2 = buildRecord(testRecord2);
        this.serializedRecord = record.serializeToBuffer();
    }

    private ImprintRecord buildRecord(DataGenerator.TestRecord pojo) {
        var builder = ImprintRecord.builder(SCHEMA_ID);
        builder.field(1, pojo.id);
        builder.field(2, pojo.timestamp);
        builder.field(3, pojo.flags);
        builder.field(4, pojo.active);
        builder.field(5, pojo.value);
        builder.field(6, pojo.data);
        builder.field(7, pojo.tags);
        builder.field(8, pojo.metadata);
        try {
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(Blackhole bh) {
        bh.consume(buildRecord(this.testData).serializeToBuffer());
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            bh.consume(ImprintRecord.deserialize(serializedRecord));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        var projected = ImprintOperations.project(record, 1, 2, 7);
        bh.consume(projected.serializeToBuffer());
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            var merged = ImprintOperations.merge(record, record2);
            bh.consume(merged.serializeToBuffer());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 