package com.imprint.benchmark.competitors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;

public class KryoCompetitor extends AbstractCompetitor {

    private final Kryo kryo;
    private byte[] serializedRecord;

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
            this.serializedRecord = baos.toByteArray();
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
        try (Input input = new Input(serializedRecord)) {
            bh.consume(kryo.readObject(input, DataGenerator.TestRecord.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        var projected = new DataGenerator.ProjectedRecord();
        projected.id = this.testData.id;
        projected.timestamp = this.testData.timestamp;
        projected.tags = this.testData.tags.subList(0, 5);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, projected);
            bh.consume(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        var merged = new DataGenerator.TestRecord();
        merged.id = this.testData.id;
        merged.timestamp = System.currentTimeMillis();
        merged.flags = this.testData.flags;
        merged.active = false;
        merged.value = this.testData.value;
        merged.data = this.testData.data;
        merged.tags = this.testData2.tags;
        merged.metadata = this.testData2.metadata;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, merged);
            bh.consume(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 