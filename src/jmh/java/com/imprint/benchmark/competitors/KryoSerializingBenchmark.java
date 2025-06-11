package com.imprint.benchmark.competitors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.imprint.benchmark.DataGenerator;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class KryoSerializingBenchmark extends AbstractSerializingBenchmark {

    private final Kryo kryo;
    private byte[] serializedRecord1;
    private byte[] serializedRecord2;

    public KryoSerializingBenchmark() {
        super("Kryo");
        this.kryo = new Kryo();
        this.kryo.register(DataGenerator.TestRecord.class);
        this.kryo.register(DataGenerator.ProjectedRecord.class);
        this.kryo.register(byte[].class);
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(Arrays.asList().getClass());
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);

        // Fix 1: Create fresh streams for each record
        this.serializedRecord1 = serializeRecord(testRecord);
        this.serializedRecord2 = serializeRecord(testRecord2);
    }

    // Helper method to properly serialize a record
    private byte[] serializeRecord(DataGenerator.TestRecord record) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, record);
            output.flush(); // Important: flush before getting bytes
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize record", e);
        }
    }

    @Override
    public void serialize(Blackhole bh) {
        // Fix 2: Create fresh output stream each time
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeObject(output, this.testData);
            output.flush(); // Ensure data is written
            bh.consume(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Serialize failed", e);
        }
    }

    @Override
    public void deserialize(Blackhole bh) {
        // Fix 3: Create fresh input each time
        try (Input input = new Input(serializedRecord1)) {
            bh.consume(kryo.readObject(input, DataGenerator.TestRecord.class));
        } catch (Exception e) {
            throw new RuntimeException("Deserialize failed", e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try {
            // Step 1: Deserialize with fresh input
            DataGenerator.TestRecord original;
            try (Input input = new Input(serializedRecord1)) {
                original = kryo.readObject(input, DataGenerator.TestRecord.class);
            }

            // Step 2: Create projected record
            var projected = new DataGenerator.ProjectedRecord();
            projected.id = original.id;
            projected.timestamp = original.timestamp;
            projected.tags = new ArrayList<>(original.tags.subList(0, Math.min(5, original.tags.size())));

            // Step 3: Serialize with fresh output
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 Output output = new Output(baos)) {
                kryo.writeObject(output, projected);
                output.flush();
                bh.consume(baos.toByteArray());
            }

        } catch (Exception e) {
            throw new RuntimeException("ProjectAndSerialize failed", e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        try {
            // Step 1: Deserialize both records with fresh inputs
            DataGenerator.TestRecord r1, r2;
            try (Input input1 = new Input(serializedRecord1)) {
                r1 = kryo.readObject(input1, DataGenerator.TestRecord.class);
            }
            try (Input input2 = new Input(serializedRecord2)) {
                r2 = kryo.readObject(input2, DataGenerator.TestRecord.class);
            }

            // Step 2: Create merged record
            var merged = new DataGenerator.TestRecord();
            merged.id = r1.id;
            merged.timestamp = System.currentTimeMillis();
            merged.flags = r1.flags;
            merged.active = false;
            merged.value = r1.value;
            merged.data = r1.data;
            merged.tags = r2.tags;
            merged.metadata = r2.metadata;

            // Step 3: Serialize with fresh output
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 Output output = new Output(baos)) {
                kryo.writeObject(output, merged);
                output.flush();
                bh.consume(baos.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException("MergeAndSerialize failed", e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        // Fix 4: Create fresh input for each access
        try (Input input = new Input(serializedRecord1)) {
            DataGenerator.TestRecord record = kryo.readObject(input, DataGenerator.TestRecord.class);
            bh.consume(record.timestamp);
        } catch (Exception e) {
            throw new RuntimeException("AccessField failed", e);
        }
    }
}