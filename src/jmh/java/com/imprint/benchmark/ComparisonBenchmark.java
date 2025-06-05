package com.imprint.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Head-to-head benchmarks comparing Imprint against other serialization libraries.
 * Tests the performance claims made in the documentation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ComparisonBenchmark {

    // Test data
    private TestRecord testData;
    
    // Serialized formats
    private ByteBuffer imprintBytes;
    private byte[] jacksonBytes;
    private byte[] kryoBytes;
    
    // Library instances
    private ObjectMapper jackson;
    private Kryo kryo;

    @Setup
    public void setup() throws Exception {
        testData = createTestRecord();
        
        // Initialize libraries
        jackson = new ObjectMapper();
        kryo = new Kryo();
        kryo.register(TestRecord.class);
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        
        // Pre-serialize for deserialization benchmarks
        imprintBytes = serializeWithImprint(testData);
        jacksonBytes = serializeWithJackson(testData);
        kryoBytes = serializeWithKryo(testData);
    }

    // ===== SERIALIZATION BENCHMARKS =====

    @Benchmark
    public void serializeImprint(Blackhole bh) throws Exception {
        ByteBuffer result = serializeWithImprint(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeJackson(Blackhole bh) throws Exception {
        byte[] result = serializeWithJackson(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeKryo(Blackhole bh) {
        byte[] result = serializeWithKryo(testData);
        bh.consume(result);
    }

    // ===== DESERIALIZATION BENCHMARKS =====

    @Benchmark
    public void deserializeImprint(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(imprintBytes.duplicate());
        bh.consume(result);
    }

    @Benchmark
    public void deserializeJackson(Blackhole bh) throws Exception {
        TestRecord result = jackson.readValue(jacksonBytes, TestRecord.class);
        bh.consume(result);
    }

    @Benchmark
    public void deserializeKryo(Blackhole bh) {
        Input input = new Input(new ByteArrayInputStream(kryoBytes));
        TestRecord result = kryo.readObject(input, TestRecord.class);
        input.close();
        bh.consume(result);
    }

    // ===== FIELD ACCESS BENCHMARKS =====
    // Tests accessing a single field near the end of a large record
    @Benchmark
    public void singleFieldAccessImprint(Blackhole bh) throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(imprintBytes.duplicate());
        
        // Access field 15 directly via directory lookup - O(1)
        var field15 = record.getValue(15);
        bh.consume(field15);
    }

    @Benchmark
    public void singleFieldAccessJackson(Blackhole bh) throws Exception {
        // Jackson must deserialize entire object to access any field
        TestRecord record = jackson.readValue(jacksonBytes, TestRecord.class);
        
        // Access field15 equivalent (extraData[4]) after full deserialization
        bh.consume(record.extraData.get(4));
    }

    @Benchmark
    public void singleFieldAccessKryo(Blackhole bh) {
        // Kryo must deserialize entire object to access any field
        Input input = new Input(new ByteArrayInputStream(kryoBytes));
        TestRecord record = kryo.readObject(input, TestRecord.class);
        input.close();
        
        // Access field15 equivalent (extraData[4]) after full deserialization
        bh.consume(record.extraData.get(4));
    }

    // ===== SIZE COMPARISON =====

    @Benchmark
    public void measureImprintSize(Blackhole bh) throws Exception {
        ByteBuffer serialized = serializeWithImprint(testData);
        bh.consume(serialized.remaining());
    }

    @Benchmark
    public void measureJacksonSize(Blackhole bh) throws Exception {
        byte[] serialized = serializeWithJackson(testData);
        bh.consume(serialized.length);
    }

    @Benchmark
    public void measureKryoSize(Blackhole bh) {
        byte[] serialized = serializeWithKryo(testData);
        bh.consume(serialized.length);
    }

    // ===== MERGE SIMULATION BENCHMARKS =====

    @Benchmark
    public void mergeImprint(Blackhole bh) throws Exception {
        var record1 = serializeWithImprint(testData);
        var record2 = serializeWithImprint(createTestRecord2());

        var deserialized1 = ImprintRecord.deserialize(record1);
        var deserialized2 = ImprintRecord.deserialize(record2);
        var merged = simulateMerge(deserialized1, deserialized2);
        
        bh.consume(merged);
    }

    @Benchmark
    public void mergeJackson(Blackhole bh) throws Exception {
        // Jackson merge requires full deserialization + merge + serialization
        var record1 = jackson.readValue(jacksonBytes, TestRecord.class);
        var record2 = jackson.readValue(serializeWithJackson(createTestRecord2()), TestRecord.class);
        
        var merged = mergeTestRecords(record1, record2);
        byte[] result = jackson.writeValueAsBytes(merged);
        
        bh.consume(result);
    }

    @Benchmark
    public void mergeKryo(Blackhole bh) {
        // Kryo merge requires full deserialization + merge + serialization
        Input input1 = new Input(new ByteArrayInputStream(kryoBytes));
        var record1 = kryo.readObject(input1, TestRecord.class);
        input1.close();
        
        Input input2 = new Input(new ByteArrayInputStream(serializeWithKryo(createTestRecord2())));
        var record2 = kryo.readObject(input2, TestRecord.class);
        input2.close();
        
        var merged = mergeTestRecords(record1, record2);
        byte[] result = serializeWithKryo(merged);
        
        bh.consume(result);
    }

    // ===== HELPER METHODS =====

    private ByteBuffer serializeWithImprint(TestRecord data) throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        writer.addField(1, Value.fromInt32(data.id));
        writer.addField(2, Value.fromString(data.name));
        writer.addField(3, Value.fromFloat64(data.price));
        writer.addField(4, Value.fromBoolean(data.active));
        writer.addField(5, Value.fromString(data.category));
        
        // Convert tags list
        var tagValues = new ArrayList<Value>();
        for (String tag : data.tags) {
            tagValues.add(Value.fromString(tag));
        }
        writer.addField(6, Value.fromArray(tagValues));
        
        // Convert metadata map
        var metadataMap = new HashMap<MapKey, Value>();
        for (var entry : data.metadata.entrySet()) {
            metadataMap.put(MapKey.fromString(entry.getKey()), Value.fromString(entry.getValue()));
        }
        writer.addField(7, Value.fromMap(metadataMap));
        
        // Add extra fields (8-20) to create a larger record
        for (int i = 0; i < data.extraData.size(); i++) {
            writer.addField(8 + i, Value.fromString(data.extraData.get(i)));
        }
        
        return writer.build().serializeToBuffer();
    }

    private byte[] serializeWithJackson(TestRecord data) throws Exception {
        return jackson.writeValueAsBytes(data);
    }

    private byte[] serializeWithKryo(TestRecord data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeObject(output, data);
        output.close();
        return baos.toByteArray();
    }

    private ImprintRecord simulateMerge(ImprintRecord first, ImprintRecord second) throws Exception {
        var writer = new ImprintWriter(first.getHeader().getSchemaId());
        var usedFieldIds = new HashSet<Integer>();
        
        // Copy fields from first record (takes precedence)
        copyFieldsToWriter(first, writer, usedFieldIds);
        copyFieldsToWriter(second, writer, usedFieldIds);
        
        return writer.build();
    }

    private void copyFieldsToWriter(ImprintRecord record, ImprintWriter writer, Set<Integer> usedFieldIds) throws Exception {
        for (var entry : record.getDirectory()) {
            int fieldId = entry.getId();
            if (!usedFieldIds.contains(fieldId)) {
                var value = record.getValue(fieldId);
                if (value != null) {
                    writer.addField(fieldId, value);
                    usedFieldIds.add(fieldId);
                }
            }
        }
    }

    private TestRecord mergeTestRecords(TestRecord first, TestRecord second) {
        // Simple merge logic - first record takes precedence
        var merged = new TestRecord();
        merged.id = first.id;
        merged.name = first.name != null ? first.name : second.name;
        merged.price = first.price != 0.0 ? first.price : second.price;
        merged.active = first.active;
        merged.category = first.category != null ? first.category : second.category;
        
        merged.tags = new ArrayList<>(first.tags);
        merged.tags.addAll(second.tags);
        
        merged.metadata = new HashMap<>(first.metadata);
        merged.metadata.putAll(second.metadata);
        
        return merged;
    }

    private TestRecord createTestRecord() {
        var record = new TestRecord();
        record.id = 12345;
        record.name = "Test Product";
        record.price = 99.99;
        record.active = true;
        record.category = "Electronics";
        
        record.tags = Arrays.asList("popular", "trending", "bestseller");
        
        record.metadata = new HashMap<>();
        record.metadata.put("manufacturer", "TechCorp");
        record.metadata.put("model", "TC-2024");
        record.metadata.put("warranty", "2 years");
        
        // Add extra data to create a larger record (fields 8-20)
        record.extraData = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            record.extraData.add("extraField" + i + "_value_" + (1000 + i));
        }
        
        return record;
    }

    private TestRecord createTestRecord2() {
        var record = new TestRecord();
        record.id = 67890;
        record.name = "Test Product 2";
        record.price = 149.99;
        record.active = false;
        record.category = "Software";
        
        record.tags = Arrays.asList("new", "premium");
        
        record.metadata = new HashMap<>();
        record.metadata.put("vendor", "SoftCorp");
        record.metadata.put("version", "2.1");
        
        // Add extra data to match the structure
        record.extraData = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            record.extraData.add("extraField" + i + "_value2_" + (2000 + i));
        }
        
        return record;
    }

    // Test data class for other serialization libraries
    public static class TestRecord {
        public int id;
        public String name;
        public double price;
        public boolean active;
        public String category;
        public List<String> tags = new ArrayList<>();
        public Map<String, String> metadata = new HashMap<>();
        public List<String> extraData = new ArrayList<>(); // Fields 8-20 for large record test
        
        public TestRecord() {} // Required for deserialization
    }
}