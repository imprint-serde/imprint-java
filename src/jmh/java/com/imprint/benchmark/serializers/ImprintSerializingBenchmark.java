package com.imprint.benchmark.serializers;

import com.imprint.benchmark.DataGenerator;
import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintRecordBuilder;
import com.imprint.core.SchemaId;
import com.imprint.error.ImprintException;
import org.openjdk.jmh.infra.Blackhole;

public class ImprintSerializingBenchmark extends AbstractSerializingBenchmark {

    private ImprintRecord imprintRecord1;
    private byte[] serializedRecord1;
    private byte[] serializedRecord2;
    private static final SchemaId SCHEMA_ID = new SchemaId(1, 1);

    public ImprintSerializingBenchmark() {
        super("Imprint");
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try {
            this.imprintRecord1 = buildRecord(testRecord).build();
            ImprintRecord imprintRecord2 = buildRecord(testRecord2).build();
            
            var buf1 = this.imprintRecord1.serializeToBuffer();
            this.serializedRecord1 = new byte[buf1.remaining()];
            buf1.get(this.serializedRecord1);

            var buf2 = imprintRecord2.serializeToBuffer();
            this.serializedRecord2 = new byte[buf2.remaining()];
            buf2.get(this.serializedRecord2);
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    private ImprintRecordBuilder buildRecord(DataGenerator.TestRecord pojo) throws ImprintException {
        var builder = ImprintRecord.builder(SCHEMA_ID);
        builder.field(0, pojo.id);
        builder.field(1, pojo.timestamp);
        builder.field(2, pojo.flags);
        builder.field(3, pojo.active);
        builder.field(4, pojo.value);
        builder.field(5, pojo.data);
        builder.field(6, pojo.tags);
        builder.field(7, pojo.metadata);
        return builder;
    }

    @Override
    public void serialize(Blackhole bh) {
        try {
            bh.consume(buildRecord(DataGenerator.createTestRecord()).buildToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            bh.consume(ImprintRecord.deserialize(this.serializedRecord1));
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            var projected = this.imprintRecord1.project(0, 1, 6);
            bh.consume(projected.serializeToBuffer());
        } catch (ImprintException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            // Use zero-copy merge - keep one record, deserialize the other
            var r2 = ImprintRecord.deserialize(this.serializedRecord2);
            var merged = this.imprintRecord1.merge(r2);
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