package com.imprint.benchmark.serializers;

import com.imprint.benchmark.DataGenerator;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;

public class ChronicleWireSerializingBenchmark extends AbstractSerializingBenchmark {

    private byte[] serializedRecord1;

    public ChronicleWireSerializingBenchmark() {
        super("Chronicle-Wire");
    }

    @Override
    public void setup(DataGenerator.TestRecord record1, DataGenerator.TestRecord record2) {
        super.setup(record1, record2);
        
        // Pre-serialize for deserialize benchmarks
        this.serializedRecord1 = serializeRecord(record1);
        byte[] serializedRecord2 = serializeRecord(record2);
    }

    @Override
    public void serialize(Blackhole bh) {
        byte[] serialized = serializeRecord(testData);
        bh.consume(serialized);
    }

    @Override
    public void deserialize(Blackhole bh) {
        DataGenerator.TestRecord deserialized = deserializeRecord(serializedRecord1);
        bh.consume(deserialized);
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        // Full round trip: deserialize, project to a new object, re-serialize
        DataGenerator.TestRecord original = deserializeRecord(serializedRecord1);
        
        // Simulate projection by creating projected object
        DataGenerator.ProjectedRecord projected = new DataGenerator.ProjectedRecord();
        projected.id = original.id;
        projected.timestamp = original.timestamp;
        projected.tags = original.tags.subList(0, Math.min(5, original.tags.size()));
        
        byte[] serialized = serializeProjectedRecord(projected);
        bh.consume(serialized);
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        // Deserialize both records, merge them, and serialize the result
        DataGenerator.TestRecord r1 = deserializeRecord(serializedRecord1);
        DataGenerator.TestRecord r2 = testData2; // Use second record directly
        
        // Create merged record following the pattern from other implementations
        DataGenerator.TestRecord merged = new DataGenerator.TestRecord();
        merged.id = r1.id;
        merged.timestamp = System.currentTimeMillis(); // new value
        merged.flags = r1.flags;
        merged.active = false; // new value
        merged.value = r1.value;
        merged.data = r1.data;
        merged.tags = r2.tags;
        merged.metadata = r2.metadata;
        
        byte[] serialized = serializeRecord(merged);
        bh.consume(serialized);
    }

    @Override
    public void accessField(Blackhole bh) {
        DataGenerator.TestRecord deserialized = deserializeRecord(serializedRecord1);
        long timestamp = deserialized.timestamp;
        bh.consume(timestamp);
    }
    
    private byte[] serializeRecord(DataGenerator.TestRecord record) {
        Bytes<?> bytes = Bytes.elasticByteBuffer();
        try {
            Wire wire = WireType.BINARY.apply(bytes);
            
            wire.writeDocument(false, w -> {
                if (record.id != null) w.write("id").text(record.id);
                w.write("timestamp").int64(record.timestamp)
                 .write("flags").int32(record.flags)
                 .write("active").bool(record.active)
                 .write("value").float64(record.value);
                 
                if (record.data != null) {
                    w.write("data").bytes(record.data);
                }
                if (record.tags != null) {
                    w.write("tags").object(record.tags);
                }
                if (record.metadata != null) {
                    w.write("metadata").marshallable(m -> {
                        for (Map.Entry<String, String> entry : record.metadata.entrySet()) {
                            m.write(entry.getKey()).text(entry.getValue());
                        }
                    });
                }
            });
            
            byte[] result = new byte[(int) bytes.readRemaining()];
            bytes.read(result);
            return result;
        } finally {
            bytes.releaseLast();
        }
    }
    
    private byte[] serializeProjectedRecord(DataGenerator.ProjectedRecord record) {
        Bytes<?> bytes = Bytes.elasticByteBuffer();
        try {
            Wire wire = WireType.BINARY.apply(bytes);
            
            wire.writeDocument(false, w -> {
                if (record.id != null) w.write("id").text(record.id);
                w.write("timestamp").int64(record.timestamp);
                if (record.tags != null) {
                    w.write("tags").object(record.tags);
                }
            });
            
            byte[] result = new byte[(int) bytes.readRemaining()];
            bytes.read(result);
            return result;
        } finally {
            bytes.releaseLast();
        }
    }
    
    private DataGenerator.TestRecord deserializeRecord(byte[] data) {
        Bytes<?> bytes = Bytes.wrapForRead(data);
        try {
            Wire wire = new BinaryWire(bytes);
            DataGenerator.TestRecord record = new DataGenerator.TestRecord();
            
            wire.readDocument(null, w -> {
                record.id = w.read("id").text();
                record.timestamp = w.read("timestamp").int64();
                record.flags = w.read("flags").int32();
                record.active = w.read("active").bool();
                record.value = w.read("value").float64();
                record.data = w.read("data").bytes();
                record.tags = (List<Integer>) w.read("tags").object();
                record.metadata = w.read("metadata").marshallableAsMap(String.class, String.class);
            });
            
            return record;
        } finally {
            bytes.releaseLast();
        }
    }
}