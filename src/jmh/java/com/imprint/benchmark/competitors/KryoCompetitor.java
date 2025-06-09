package com.imprint.benchmark.competitors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

public class KryoCompetitor extends AbstractCompetitor {

    private final Kryo kryo;
    private byte[] serializedRecord1;
    private byte[] serializedRecord2;

    public KryoCompetitor() {
        super("Kryo");
        this.kryo = new Kryo();
        this.kryo.register(DataGenerator.TestRecord.class);
        this.kryo.register(DataGenerator.ProjectedRecord.class);
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, testRecord);
            this.serializedRecord1 = baos.toByteArray();
            baos.reset();
            kryo.writeObject(output, testRecord2);
            this.serializedRecord2 = baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(Blackhole bh) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, this.testData);
            bh.consume(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        try (Input input = new Input(serializedRecord1)) {
            bh.consume(kryo.readObject(input, DataGenerator.TestRecord.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        // Full round trip: deserialize, project to a new object, re-serialize
        try (Input input = new Input(serializedRecord1)) {
            DataGenerator.TestRecord original = kryo.readObject(input, DataGenerator.TestRecord.class);

            var projected = new DataGenerator.ProjectedRecord();
            projected.id = original.id;
            projected.timestamp = original.timestamp;
            projected.tags = new ArrayList<>(original.tags.subList(0, 5));

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 Output output = new Output(baos)) {
                kryo.writeObject(output, projected);
                bh.consume(baos.toByteArray());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            DataGenerator.TestRecord r1, r2;
            try (Input input = new Input(serializedRecord1)) {
                r1 = kryo.readObject(input, DataGenerator.TestRecord.class);
            }
            try (Input input = new Input(serializedRecord2)) {
                r2 = kryo.readObject(input, DataGenerator.TestRecord.class);
            }

            var merged = new DataGenerator.TestRecord();
            merged.id = r1.id;
            merged.timestamp = System.currentTimeMillis();
            merged.flags = r1.flags;
            merged.active = false;
            merged.value = r1.value;
            merged.data = r1.data;
            merged.tags = r2.tags;
            merged.metadata = r2.metadata;

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 Output output = new Output(baos)) {
                kryo.writeObject(output, merged);
                bh.consume(baos.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try (Input input = new Input(serializedRecord1)) {
            DataGenerator.TestRecord record = kryo.readObject(input, DataGenerator.TestRecord.class);
            bh.consume(record.timestamp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 