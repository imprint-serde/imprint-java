package com.imprint.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.flatbuffers.FlatBufferBuilder;
import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings("unused")
public class ComparisonBenchmark {

    // Test data
    private TestRecord testData;

    // Serialized formats
    private ByteBuffer imprintBytesBuffer;
    private byte[] jacksonJsonBytes;
    private byte[] kryoBytes;
    private byte[] messagePackBytes;
    private byte[] avroBytes;
    private byte[] protobufBytes;
    private ByteBuffer flatbuffersBytes;

    // Library instances
    private Schema avroSchema;
    private DatumWriter<GenericRecord> avroWriter;
    private DatumReader<GenericRecord> avroReader;
    private ObjectMapper jacksonJsonMapper;
    private Kryo kryo;
    private ObjectMapper messagePackMapper;

    @Setup
    public void setup() throws Exception {
        testData = createTestRecord();

        // Initialize libraries
        jacksonJsonMapper = new ObjectMapper();
        kryo = new Kryo();
        kryo.register(TestRecord.class);
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(Arrays.asList().getClass());

        // Initialize MessagePack ObjectMapper
        messagePackMapper = new ObjectMapper(new MessagePackFactory());
        setupAvro();

        // Pre-serialize for deserialization benchmarks
        imprintBytesBuffer = serializeWithImprint(testData);
        jacksonJsonBytes = serializeWithJacksonJson(testData);
        kryoBytes = serializeWithKryo(testData);
        messagePackBytes = serializeWithMessagePack(testData);
        avroBytes = serializeWithAvro(testData);
        protobufBytes = serializeWithProtobuf(testData);
        flatbuffersBytes = serializeWithFlatBuffers(testData);
    }

    // ===== SERIALIZATION BENCHMARKS =====

    @Benchmark
    public void serializeImprint(Blackhole bh) throws Exception {
        ByteBuffer result = serializeWithImprint(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeJacksonJson(Blackhole bh) throws Exception {
        byte[] result = serializeWithJacksonJson(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeKryo(Blackhole bh) {
        byte[] result = serializeWithKryo(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeMessagePack(Blackhole bh) throws Exception {
        byte[] result = serializeWithMessagePack(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeAvro(Blackhole bh) throws Exception {
        byte[] result = serializeWithAvro(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeProtobuf(Blackhole bh) {
        byte[] result = serializeWithProtobuf(testData);
        bh.consume(result);
    }

    @Benchmark
    public void serializeFlatBuffers(Blackhole bh) {
        ByteBuffer result = serializeWithFlatBuffers(testData);
        bh.consume(result);
    }

    // ===== SETUP ONLY =====

    @Benchmark
    public void deserializeSetupImprint(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(imprintBytesBuffer.duplicate());
        bh.consume(result);
    }

    @Benchmark
    public void deserializeSetupFlatBuffers(Blackhole bh) {
        TestRecordFB result = TestRecordFB.getRootAsTestRecordFB(flatbuffersBytes.duplicate());
        bh.consume(result);
    }

    // ===== FULL DESERIALIZATION BENCHMARKS =====

    @Benchmark
    public void deserializeJacksonJson(Blackhole bh) throws Exception {
        TestRecord result = jacksonJsonMapper.readValue(jacksonJsonBytes, TestRecord.class);
        bh.consume(result);
    }

    @Benchmark
    public void deserializeKryo(Blackhole bh) {
        Input input = new Input(new ByteArrayInputStream(kryoBytes));
        TestRecord result = kryo.readObject(input, TestRecord.class);
        input.close();
        bh.consume(result);
    }

    @Benchmark
    public void deserializeMessagePack(Blackhole bh) throws Exception {
        TestRecord result = messagePackMapper.readValue(messagePackBytes, TestRecord.class);
        bh.consume(result);
    }

    @Benchmark
    public void deserializeAvro(Blackhole bh) throws Exception {
        GenericRecord result = deserializeWithAvro(avroBytes);
        bh.consume(result);
    }

    @Benchmark
    public void deserializeProtobuf(Blackhole bh) throws Exception {
        TestRecordProto.TestRecord result = TestRecordProto.TestRecord.parseFrom(protobufBytes);
        bh.consume(result);
    }

    @Benchmark
    public void deserializeImprint(Blackhole bh) throws Exception {
        ImprintRecord result = ImprintRecord.deserialize(imprintBytesBuffer.duplicate());
        // Access all fields to force full deserialization
        result.getInt32(1);        // id
        result.getString(2);       // name
        result.getFloat64(3);      // price
        result.getBoolean(4);      // active
        result.getString(5);       // category
        result.getArray(6);        // tags
        result.getMap(7);          // metadata
        for (int i = 8; i < 21; i++) {
            result.getString(i);   // extraData fields
        }

        bh.consume(result);
    }

    @Benchmark
    public void deserializeFlatBuffers(Blackhole bh) {
        TestRecordFB result = TestRecordFB.getRootAsTestRecordFB(flatbuffersBytes.duplicate());

        // Access all fields
        result.id();
        result.name();
        result.price();
        result.active();
        result.category();
        // Access all tags
        for (int i = 0; i < result.tagsLength(); i++) {
            result.tags(i);
        }
        // Access all metadata
        for (int i = 0; i < result.metadataKeysLength(); i++) {
            result.metadataKeys(i);
            result.metadataValues(i);
        }
        // Access all extra data
        for (int i = 0; i < result.extraDataLength(); i++) {
            result.extraData(i);
        }

        bh.consume(result);
    }

    // ===== FIELD ACCESS BENCHMARKS =====
    // Tests accessing a single field near the end of a record

    @Benchmark
    public void singleFieldAccessImprint(Blackhole bh) throws Exception {
        ImprintRecord record = ImprintRecord.deserialize(imprintBytesBuffer.duplicate());
        var field15 = record.getString(15);
        bh.consume(field15);
    }

    @Benchmark
    public void singleFieldAccessJacksonJson(Blackhole bh) throws Exception {
        TestRecord record = jacksonJsonMapper.readValue(jacksonJsonBytes, TestRecord.class);
        bh.consume(record.extraData.get(4));
    }

    @Benchmark
    public void singleFieldAccessKryo(Blackhole bh) {
        Input input = new Input(new ByteArrayInputStream(kryoBytes));
        TestRecord record = kryo.readObject(input, TestRecord.class);
        input.close();
        bh.consume(record.extraData.get(4));
    }

    @Benchmark
    public void singleFieldAccessMessagePack(Blackhole bh) throws Exception {
        TestRecord record = messagePackMapper.readValue(messagePackBytes, TestRecord.class);
        bh.consume(record.extraData.get(4));
    }

    @Benchmark
    public void singleFieldAccessAvro(Blackhole bh) throws Exception {
        GenericRecord record = deserializeWithAvro(avroBytes);
        bh.consume(record.get("extraData4")); // Accessing field near end
    }

    @Benchmark
    public void singleFieldAccessProtobuf(Blackhole bh) throws Exception {
        TestRecordProto.TestRecord record = TestRecordProto.TestRecord.parseFrom(protobufBytes);
        bh.consume(record.getExtraData(4)); // Accessing field near end
    }

    @Benchmark
    public void singleFieldAccessFlatBuffers(Blackhole bh) {
        TestRecordFB record = TestRecordFB.getRootAsTestRecordFB(flatbuffersBytes.duplicate());
        bh.consume(record.extraData(4)); // Accessing field near end - zero copy!
    }

    // ===== SIZE COMPARISON =====

    @Benchmark
    public void measureImprintSize(Blackhole bh) {
        bh.consume(imprintBytesBuffer.remaining());
    }

    @Benchmark
    public void measureJacksonJsonSize(Blackhole bh) {
        bh.consume(jacksonJsonBytes.length);
    }

    @Benchmark
    public void measureKryoSize(Blackhole bh) {
        bh.consume(kryoBytes.length);
    }

    @Benchmark
    public void measureMessagePackSize(Blackhole bh) {
        bh.consume(messagePackBytes.length);
    }

    @Benchmark
    public void measureAvroSize(Blackhole bh) {
        bh.consume(avroBytes.length);
    }

    @Benchmark
    public void measureProtobufSize(Blackhole bh) {
        bh.consume(protobufBytes.length);
    }

    @Benchmark
    public void measureFlatBuffersSize(Blackhole bh) {
        bh.consume(flatbuffersBytes.remaining());
    }

    // ===== MERGE SIMULATION BENCHMARKS =====

    //@Benchmark
    public void mergeImprint(Blackhole bh) throws Exception {
        var record1Buffer = imprintBytesBuffer.duplicate();
        var record2Data = createTestRecord2();
        var record2Buffer = serializeWithImprint(record2Data);

        var deserialized1 = ImprintRecord.deserialize(record1Buffer);
        var deserialized2 = ImprintRecord.deserialize(record2Buffer);
        var merged = simulateMerge(deserialized1, deserialized2);

        bh.consume(merged);
    }

    //@Benchmark
    public void mergeJacksonJson(Blackhole bh) throws Exception {
        var record1 = jacksonJsonMapper.readValue(jacksonJsonBytes, TestRecord.class);
        var record2Data = createTestRecord2();
        var record2Bytes = serializeWithJacksonJson(record2Data);
        var record2 = jacksonJsonMapper.readValue(record2Bytes, TestRecord.class);

        var mergedPojo = mergeTestRecords(record1, record2);
        byte[] result = jacksonJsonMapper.writeValueAsBytes(mergedPojo);
        bh.consume(result);
    }

    //@Benchmark
    public void mergeKryo(Blackhole bh) {
        Input input1 = new Input(new ByteArrayInputStream(kryoBytes));
        var record1 = kryo.readObject(input1, TestRecord.class);
        input1.close();

        var record2Data = createTestRecord2();
        var record2Bytes = serializeWithKryo(record2Data);
        Input input2 = new Input(new ByteArrayInputStream(record2Bytes));
        var record2 = kryo.readObject(input2, TestRecord.class);
        input2.close();

        var mergedPojo = mergeTestRecords(record1, record2);
        byte[] result = serializeWithKryo(mergedPojo);
        bh.consume(result);
    }

    //@Benchmark
    public void mergeMessagePack(Blackhole bh) throws Exception {
        var record1 = messagePackMapper.readValue(messagePackBytes, TestRecord.class);
        var record2Data = createTestRecord2();
        var record2Bytes = serializeWithMessagePack(record2Data);
        var record2 = messagePackMapper.readValue(record2Bytes, TestRecord.class);

        var mergedPojo = mergeTestRecords(record1, record2);
        byte[] result = messagePackMapper.writeValueAsBytes(mergedPojo);
        bh.consume(result);
    }

    //@Benchmark
    public void mergeAvro(Blackhole bh) throws Exception {
        var record1 = deserializeWithAvro(avroBytes);
        var record2Data = createTestRecord2();
        var record2Bytes = serializeWithAvro(record2Data);
        var record2 = deserializeWithAvro(record2Bytes);

        var merged = mergeAvroRecords(record1, record2);
        byte[] result = serializeAvroRecord(merged);
        bh.consume(result);
    }

    //@Benchmark
    public void mergeProtobuf(Blackhole bh) throws Exception {
        var record1 = TestRecordProto.TestRecord.parseFrom(protobufBytes);
        var record2Data = createTestRecord2();
        var record2Bytes = serializeWithProtobuf(record2Data);
        var record2 = TestRecordProto.TestRecord.parseFrom(record2Bytes);

        var merged = mergeProtobufRecords(record1, record2);
        byte[] result = merged.toByteArray();
        bh.consume(result);
    }

    //@Benchmark
    public void mergeFlatBuffers(Blackhole bh) {
        var record1 = TestRecordFB.getRootAsTestRecordFB(flatbuffersBytes.duplicate());
        var record2Data = createTestRecord2();
        var record2Buffer = serializeWithFlatBuffers(record2Data);
        var record2 = TestRecordFB.getRootAsTestRecordFB(record2Buffer);

        var merged = mergeFlatBuffersRecords(record1, record2);
        bh.consume(merged);
    }

    // ===== MAIN METHOD TO RUN BENCHMARKS =====

    public static void main(String[] args) throws RunnerException {
        runAll();
        // Or, uncomment specific runner methods to execute subsets:
        // runSerializationBenchmarks();
        // runDeserializationBenchmarks();
        // runFieldAccessBenchmarks();
        // runSizeComparisonBenchmarks();
        // runMergeBenchmarks();
        // runMessagePackBenchmarks();
    }

    public static void runAll() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    public static void runSerializationBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".serialize.*")
                .build();
        new Runner(opt).run();
    }

    public static void runDeserializationBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".deserialize.*")
                .build();
        new Runner(opt).run();
    }

    public static void runFieldAccessBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".singleFieldAccess.*")
                .build();
        new Runner(opt).run();
    }

    public static void runSizeComparisonBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".measure.*")
                .build();
        new Runner(opt).run();
    }

    public static void runMergeBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".merge.*")
                .build();
        new Runner(opt).run();
    }

    public static void runMessagePackBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".*MessagePack.*")
                .build();
        new Runner(opt).run();
    }

    public static void runAvroBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".*Avro.*")
                .build();
        new Runner(opt).run();
    }

    public static void runProtobufBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".*Protobuf.*")
                .build();
        new Runner(opt).run();
    }

    public static void runFlatBuffersBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ComparisonBenchmark.class.getSimpleName() + ".*FlatBuffers.*")
                .build();
        new Runner(opt).run();
    }

    // ===== HELPER METHODS =====

    private void setupAvro() {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"price\", \"type\": \"double\"},\n" +
                "    {\"name\": \"active\", \"type\": \"boolean\"},\n" +
                "    {\"name\": \"category\", \"type\": \"string\"},\n" +
                "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
                "    {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n" +
                "    {\"name\": \"extraData0\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData1\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData2\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData3\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData4\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData5\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData6\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData7\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData8\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData9\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData10\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData11\", \"type\": \"string\"},\n" +
                "    {\"name\": \"extraData12\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";

        avroSchema = new Schema.Parser().parse(schemaJson);
        avroWriter = new GenericDatumWriter<>(avroSchema);
        avroReader = new GenericDatumReader<>(avroSchema);
    }

    private ByteBuffer serializeWithImprint(TestRecord data) throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));

        writer.addField(1, Value.fromInt32(data.id));
        writer.addField(2, Value.fromString(data.name));
        writer.addField(3, Value.fromFloat64(data.price));
        writer.addField(4, Value.fromBoolean(data.active));
        writer.addField(5, Value.fromString(data.category));

        var tagValues = new ArrayList<Value>();
        if (data.tags != null) {
            for (String tag : data.tags) {
                tagValues.add(Value.fromString(tag));
            }
        }
        writer.addField(6, Value.fromArray(tagValues));

        var metadataMap = new HashMap<MapKey, Value>();
        if (data.metadata != null) {
            for (var entry : data.metadata.entrySet()) {
                metadataMap.put(MapKey.fromString(entry.getKey()), Value.fromString(entry.getValue()));
            }
        }
        writer.addField(7, Value.fromMap(metadataMap));

        if (data.extraData != null) {
            for (int i = 0; i < data.extraData.size(); i++) {
                writer.addField(8 + i, Value.fromString(data.extraData.get(i)));
            }
        }

        return writer.build().serializeToBuffer();
    }

    private byte[] serializeWithJacksonJson(TestRecord data) throws Exception {
        return jacksonJsonMapper.writeValueAsBytes(data);
    }

    private byte[] serializeWithKryo(TestRecord data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeObject(output, data);
        output.close();
        return baos.toByteArray();
    }

    private byte[] serializeWithMessagePack(TestRecord data) throws Exception {
        return messagePackMapper.writeValueAsBytes(data);
    }

    private byte[] serializeWithAvro(TestRecord data) throws Exception {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", data.id);
        record.put("name", data.name);
        record.put("price", data.price);
        record.put("active", data.active);
        record.put("category", data.category);
        record.put("tags", data.tags);
        record.put("metadata", data.metadata);

        for (int i = 0; i < data.extraData.size(); i++) {
            record.put("extraData" + i, data.extraData.get(i));
        }

        return serializeAvroRecord(record);
    }

    private byte[] serializeAvroRecord(GenericRecord record) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        avroWriter.write(record, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

    private GenericRecord deserializeWithAvro(byte[] data) throws Exception {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return avroReader.read(null, decoder);
    }

    private byte[] serializeWithProtobuf(TestRecord data) {
        var builder = TestRecordProto.TestRecord.newBuilder()
                .setId(data.id)
                .setName(data.name)
                .setPrice(data.price)
                .setActive(data.active)
                .setCategory(data.category)
                .addAllTags(data.tags)
                .putAllMetadata(data.metadata);

        for (String extraData : data.extraData) {
            builder.addExtraData(extraData);
        }

        return builder.build().toByteArray();
    }

    private ByteBuffer serializeWithFlatBuffers(TestRecord data) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        // Create strings (must be created before the object that uses them)
        int nameOffset = builder.createString(data.name);
        int categoryOffset = builder.createString(data.category);

        // Create tags array
        int[] tagOffsets = new int[data.tags.size()];
        for (int i = 0; i < data.tags.size(); i++) {
            tagOffsets[i] = builder.createString(data.tags.get(i));
        }
        int tagsOffset = TestRecordFB.createTagsVector(builder, tagOffsets);

        // Create metadata (as parallel arrays for keys and values)
        String[] metadataKeys = data.metadata.keySet().toArray(new String[0]);
        String[] metadataValues = new String[metadataKeys.length];
        int[] keyOffsets = new int[metadataKeys.length];
        int[] valueOffsets = new int[metadataKeys.length];

        for (int i = 0; i < metadataKeys.length; i++) {
            metadataValues[i] = data.metadata.get(metadataKeys[i]);
            keyOffsets[i] = builder.createString(metadataKeys[i]);
            valueOffsets[i] = builder.createString(metadataValues[i]);
        }
        int metadataKeysOffset = TestRecordFB.createMetadataKeysVector(builder, keyOffsets);
        int metadataValuesOffset = TestRecordFB.createMetadataValuesVector(builder, valueOffsets);

        // Create extra data array
        int[] extraDataOffsets = new int[data.extraData.size()];
        for (int i = 0; i < data.extraData.size(); i++) {
            extraDataOffsets[i] = builder.createString(data.extraData.get(i));
        }
        int extraDataOffset = TestRecordFB.createExtraDataVector(builder, extraDataOffsets);

        // Create the main object
        TestRecordFB.startTestRecordFB(builder);
        TestRecordFB.addId(builder, data.id);
        TestRecordFB.addName(builder, nameOffset);
        TestRecordFB.addPrice(builder, data.price);
        TestRecordFB.addActive(builder, data.active);
        TestRecordFB.addCategory(builder, categoryOffset);
        TestRecordFB.addTags(builder, tagsOffset);
        TestRecordFB.addMetadataKeys(builder, metadataKeysOffset);
        TestRecordFB.addMetadataValues(builder, metadataValuesOffset);
        TestRecordFB.addExtraData(builder, extraDataOffset);
        int recordOffset = TestRecordFB.endTestRecordFB(builder);

        // Finish and return
        builder.finish(recordOffset);
        return builder.dataBuffer().slice();
    }

    private ImprintRecord simulateMerge(ImprintRecord first, ImprintRecord second) throws Exception {
        var writer = new ImprintWriter(first.getHeader().getSchemaId());
        var usedFieldIds = new HashSet<Integer>();

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

    private GenericRecord mergeAvroRecords(GenericRecord first, GenericRecord second) {
        GenericRecord merged = new GenericData.Record(avroSchema);

        // Copy all fields from first record
        for (Schema.Field field : avroSchema.getFields()) {
            merged.put(field.name(), first.get(field.name()));
        }

        // Override with non-null values from second record
        for (Schema.Field field : avroSchema.getFields()) {
            Object secondValue = second.get(field.name());
            if (secondValue != null && !secondValue.toString().isEmpty()) {
                merged.put(field.name(), secondValue);
            }
        }

        return merged;
    }

    private TestRecordProto.TestRecord mergeProtobufRecords(TestRecordProto.TestRecord first, TestRecordProto.TestRecord second) {
        return TestRecordProto.TestRecord.newBuilder()
                .mergeFrom(first)
                .mergeFrom(second)
                .build();
    }

    private ByteBuffer mergeFlatBuffersRecords(TestRecordFB first, TestRecordFB second) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);

        // Use second record's values if they exist, otherwise first record's values
        String name = second.name() != null && !second.name().isEmpty() ? second.name() : first.name();
        String category = second.category() != null && !second.category().isEmpty() ? second.category() : first.category();
        double price = second.price() != 0.0 ? second.price() : first.price();
        boolean active = second.active(); // Use second's boolean value
        int id = first.id(); // Keep first record's ID

        // Create merged strings
        int nameOffset = builder.createString(name);
        int categoryOffset = builder.createString(category);

        // Merge tags (combine both arrays)
        List<String> mergedTags = new ArrayList<>();
        for (int i = 0; i < first.tagsLength(); i++) {
            mergedTags.add(first.tags(i));
        }
        for (int i = 0; i < second.tagsLength(); i++) {
            mergedTags.add(second.tags(i));
        }

        int[] tagOffsets = new int[mergedTags.size()];
        for (int i = 0; i < mergedTags.size(); i++) {
            tagOffsets[i] = builder.createString(mergedTags.get(i));
        }
        int tagsOffset = TestRecordFB.createTagsVector(builder, tagOffsets);

        // Merge metadata (second overwrites first)
        Map<String, String> mergedMetadata = new HashMap<>();
        for (int i = 0; i < first.metadataKeysLength(); i++) {
            mergedMetadata.put(first.metadataKeys(i), first.metadataValues(i));
        }
        for (int i = 0; i < second.metadataKeysLength(); i++) {
            mergedMetadata.put(second.metadataKeys(i), second.metadataValues(i));
        }

        String[] metadataKeys = mergedMetadata.keySet().toArray(new String[0]);
        int[] keyOffsets = new int[metadataKeys.length];
        int[] valueOffsets = new int[metadataKeys.length];

        for (int i = 0; i < metadataKeys.length; i++) {
            keyOffsets[i] = builder.createString(metadataKeys[i]);
            valueOffsets[i] = builder.createString(mergedMetadata.get(metadataKeys[i]));
        }
        int metadataKeysOffset = TestRecordFB.createMetadataKeysVector(builder, keyOffsets);
        int metadataValuesOffset = TestRecordFB.createMetadataValuesVector(builder, valueOffsets);

        // Use first record's extra data (or could merge both)
        int[] extraDataOffsets = new int[first.extraDataLength()];
        for (int i = 0; i < first.extraDataLength(); i++) {
            extraDataOffsets[i] = builder.createString(first.extraData(i));
        }
        int extraDataOffset = TestRecordFB.createExtraDataVector(builder, extraDataOffsets);

        // Create the merged object
        TestRecordFB.startTestRecordFB(builder);
        TestRecordFB.addId(builder, id);
        TestRecordFB.addName(builder, nameOffset);
        TestRecordFB.addPrice(builder, price);
        TestRecordFB.addActive(builder, active);
        TestRecordFB.addCategory(builder, categoryOffset);
        TestRecordFB.addTags(builder, tagsOffset);
        TestRecordFB.addMetadataKeys(builder, metadataKeysOffset);
        TestRecordFB.addMetadataValues(builder, metadataValuesOffset);
        TestRecordFB.addExtraData(builder, extraDataOffset);
        int recordOffset = TestRecordFB.endTestRecordFB(builder);

        builder.finish(recordOffset);
        return builder.dataBuffer().slice();
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