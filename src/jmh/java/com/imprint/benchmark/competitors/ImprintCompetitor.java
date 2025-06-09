package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import com.imprint.core.ImprintOperations;
import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.error.ImprintException;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;

public class ImprintCompetitor extends AbstractCompetitor {

    private ImprintRecord imprintRecord1;
    private ImprintRecord imprintRecord2;
    private static final SchemaId SCHEMA_ID = new SchemaId(1, 1);

    public ImprintCompetitor() {
        super("Imprint");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try {
            this.imprintRecord1 = buildRecord(testRecord);
            this.imprintRecord2 = buildRecord(testRecord2);
            this.serializedRecord = imprintRecord1.serializeToBuffer().array();
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    private ImprintRecord buildRecord(DataGenerator.TestRecord pojo) throws ImprintException {
        var builder = ImprintRecord.builder(SCHEMA_ID);
        builder.field(0, pojo.id);
        builder.field(1, pojo.timestamp);
        builder.field(2, pojo.flags);
        builder.field(3, pojo.active);
        builder.field(4, pojo.value);
        builder.field(5, pojo.data);
        builder.field(6, pojo.tags);
        builder.field(7, pojo.metadata);
        return builder.build();
    }

    @Override
    public void serialize(Blackhole bh) {
        try {
            bh.consume(buildRecord(this.testData).serializeToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            bh.consume(ImprintRecord.deserialize(this.serializedRecord));
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        bh.consume(imprintRecord1.project(0, 1, 6).serializeToBuffer());
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            var merged = ImprintOperations.merge(this.imprintRecord1, this.imprintRecord2);
            bh.consume(merged.serializeToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try {
            bh.consume(imprintRecord1.getInt64(1)); // Access timestamp by field ID
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }
} 