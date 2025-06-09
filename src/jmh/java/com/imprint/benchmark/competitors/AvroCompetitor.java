package com.imprint.benchmark.competitors;

import com.imprint.benchmark.DataGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class AvroCompetitor extends AbstractCompetitor {

    private final Schema schema;
    private final Schema projectedSchema;
    private final DatumWriter<GenericRecord> writer;
    private final DatumReader<GenericRecord> reader;
    private final DatumWriter<GenericRecord> projectedWriter;
    private byte[] serializedRecord1;
    private byte[] serializedRecord2;

    public AvroCompetitor() {
        super("Avro-Generic");
        String schemaDefinition = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"string\"},"
                + "{\"name\":\"timestamp\",\"type\":\"long\"},"
                + "{\"name\":\"flags\",\"type\":\"int\"},"
                + "{\"name\":\"active\",\"type\":\"boolean\"},"
                + "{\"name\":\"value\",\"type\":\"double\"},"
                + "{\"name\":\"data\",\"type\":\"bytes\"},"
                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},"
                + "{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
                + "]}";
        this.schema = new Schema.Parser().parse(schemaDefinition);
        this.writer = new GenericDatumWriter<>(schema);
        this.reader = new GenericDatumReader<>(schema);

        String projectedSchemaDef = "{\"type\":\"record\",\"name\":\"ProjectedRecord\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"string\"},"
                + "{\"name\":\"timestamp\",\"type\":\"long\"},"
                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}"
                + "]}";
        this.projectedSchema = new Schema.Parser().parse(projectedSchemaDef);
        this.projectedWriter = new GenericDatumWriter<>(projectedSchema);
    }

    @Override
    public void setup(DataGenerator.TestRecord testRecord, DataGenerator.TestRecord testRecord2) {
        super.setup(testRecord, testRecord2);
        this.serializedRecord1 = buildRecord(testRecord);
        this.serializedRecord2 = buildRecord(testRecord2);
    }

    private byte[] buildRecord(DataGenerator.TestRecord pojo) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", pojo.id);
        record.put("timestamp", pojo.timestamp);
        record.put("flags", pojo.flags);
        record.put("active", pojo.active);
        record.put("value", pojo.value);
        record.put("data", ByteBuffer.wrap(pojo.data));
        record.put("tags", pojo.tags);
        record.put("metadata", pojo.metadata);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(Blackhole bh) {
        bh.consume(buildRecord(this.testData));
    }

    @Override
    public void deserialize(Blackhole bh) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord1, null);
            bh.consume(reader.read(null, decoder));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Full round trip: deserialize, project to a new object, re-serialize
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord1, null);
            GenericRecord original = reader.read(null, decoder);
            
            // With generic records, we can project by building a new record with the projected schema
            GenericRecord projected = new GenericData.Record(projectedSchema);
            projected.put("id", original.get("id"));
            projected.put("timestamp", original.get("timestamp"));
            projected.put("tags", ((java.util.List)original.get("tags")).subList(0, 5));

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            projectedWriter.write(projected, encoder);
            encoder.flush();
            bh.consume(out.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mergeAndSerialize(Blackhole bh) {
        // No direct merge in Avro. Must deserialize, merge manually, and re-serialize.
        GenericRecord r1 = buildAvroRecordFromBytes(this.serializedRecord1);
        GenericRecord r2 = buildAvroRecordFromBytes(this.serializedRecord2);

        GenericRecord merged = new GenericData.Record(schema);
        // Simplified merge logic: take most fields from r1, some from r2
        merged.put("id", r1.get("id"));
        merged.put("timestamp", System.currentTimeMillis());
        merged.put("flags", r1.get("flags"));
        merged.put("active", false);
        merged.put("value", r1.get("value"));
        merged.put("data", r1.get("data"));
        merged.put("tags", r2.get("tags"));
        merged.put("metadata", r2.get("metadata"));
        
        bh.consume(buildBytes(merged));
    }

    private GenericRecord buildAvroRecord(DataGenerator.TestRecord pojo) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", pojo.id);
        record.put("timestamp", pojo.timestamp);
        record.put("flags", pojo.flags);
        record.put("active", pojo.active);
        record.put("value", pojo.value);
        record.put("data", ByteBuffer.wrap(pojo.data));
        record.put("tags", pojo.tags);
        record.put("metadata", pojo.metadata);
        return record;
    }

    private GenericRecord buildAvroRecordFromBytes(byte[] bytes) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] buildBytes(GenericRecord record) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accessField(Blackhole bh) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord1, null);
            GenericRecord record = reader.read(null, decoder);
            bh.consume(record.get("timestamp"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
} 