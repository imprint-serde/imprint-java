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
import java.util.stream.Collectors;

public class AvroCompetitor extends AbstractCompetitor {

    private final Schema schema;
    private final Schema projectedSchema;
    private final DatumWriter<GenericRecord> writer;
    private final DatumReader<GenericRecord> reader;
    private final DatumWriter<GenericRecord> projectedWriter;
    private byte[] serializedRecord;

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
        this.serializedRecord = buildRecord(testRecord);
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
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord, null);
            bh.consume(reader.read(null, decoder));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void projectAndSerialize(Blackhole bh) {
        // With generic records, we can project by building a new record with the projected schema
        GenericRecord projected = new GenericData.Record(projectedSchema);
        projected.put("id", this.testData.id);
        projected.put("timestamp", this.testData.timestamp);
        projected.put("tags", this.testData.tags.stream().limit(5).collect(Collectors.toList()));

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
        GenericRecord r1 = (GenericRecord) buildAvroRecord(this.testData);
        GenericRecord r2 = (GenericRecord) buildAvroRecord(this.testData2);

        GenericRecord merged = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object val = r1.get(field.name());
            if (field.name().equals("timestamp")) {
                val = System.currentTimeMillis();
            } else if(field.name().equals("active")) {
                val = false;
            } else if (r2.hasField(field.name()) && r2.get(field.name()) != null) {
                 if(!r1.hasField(field.name()) || r1.get(field.name()) == null){
                     val = r2.get(field.name());
                 }
            }
            merged.put(field.name(), val);
        }
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
} 