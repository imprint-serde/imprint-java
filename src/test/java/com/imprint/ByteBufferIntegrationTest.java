package com.imprint;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.Value;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Integration test to verify ByteBuffer functionality and zero-copy benefits.
 */
class ByteBufferIntegrationTest {

    public static void main(String[] args) {
        try {
            testByteBufferFunctionality();
            testZeroCopy();
            testArrayBackedBuffers();
            System.out.println("All ByteBuffer integration tests passed!");
        } catch (Exception e) {
            System.err.println("ByteBuffer integration test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    static void testByteBufferFunctionality() throws ImprintException {
        System.out.println("Testing ByteBuffer functionality...");

        SchemaId schemaId = new SchemaId(1, 0xdeadbeef);
        ImprintWriter writer = new ImprintWriter(schemaId);

        writer.addField(1, Value.fromInt32(42))
              .addField(2, Value.fromString("zero-copy test"))
              .addField(3, Value.fromBytes(new byte[]{1, 2, 3, 4, 5}));

        ImprintRecord record = writer.build();

        // Test ByteBuffer serialization
        ByteBuffer serializedBuffer = record.serializeToBuffer();
        assert serializedBuffer.isReadOnly() : "Serialized buffer should be read-only";

        // Test deserialization from ByteBuffer
        ImprintRecord deserialized = ImprintRecord.deserialize(serializedBuffer);

        assert Objects.equals(deserialized.getValue(1), Value.fromInt32(42));
        assert Objects.equals(deserialized.getValue(2), Value.fromString("zero-copy test"));

        // Test raw bytes access returns ByteBuffer
        var rawBytes = deserialized.getRawBytes(1);
        assert rawBytes != null : "Raw bytes should be present for field 1";
        assert rawBytes.isReadOnly() : "Raw bytes buffer should be read-only";

        System.out.println("ByteBuffer functionality test passed");
    }

    static void testZeroCopy() {
        System.out.println("Testing zero-copy");

        // Create a large payload to demonstrate zero-copy benefits
        byte[] largePayload = new byte[1024 * 1024]; // 1MB
        Arrays.fill(largePayload, (byte) 0xAB);

        SchemaId schemaId = new SchemaId(2, 0xcafebabe);
        ImprintWriter writer = new ImprintWriter(schemaId);

        try {
            writer.addField(1, Value.fromBytes(largePayload));
            ImprintRecord record = writer.build();

            // Test that getRawBytes returns a view, not a copy
            var rawBytes = record.getRawBytes(1);
            assert rawBytes !=null : "Raw bytes should be present";
            assert rawBytes.isReadOnly() : "Raw buffer should be read-only";

            // The buffer should be positioned at the start of the actual data
            // (after the VarInt length prefix)
            assert rawBytes.remaining() > largePayload.length : "Buffer should include length prefix";

            System.out.println("Zero-copy benefits test passed");

        } catch (ImprintException e) {
            throw new RuntimeException("Failed zero-copy test", e);
        }
    }

    static void testArrayBackedBuffers() throws ImprintException {
        System.out.println("Testing array-backed buffers for zero-copy performance...");

        // Test serialized buffers are array-backed
        SchemaId schemaId = new SchemaId(1, 0xdeadbeef);
        ImprintWriter writer = new ImprintWriter(schemaId);

        writer.addField(1, Value.fromInt32(42))
              .addField(2, Value.fromString("test string"))
              .addField(3, Value.fromBytes(new byte[]{1, 2, 3, 4}))
              .addField(4, Value.fromBoolean(true));

        ImprintRecord record = writer.build();

        // Test that serializeToBuffer() returns array-backed buffer
        ByteBuffer serializedBuffer = record.serializeToBuffer();
        assert serializedBuffer.hasArray() : "Serialized buffer should be array-backed for zero-copy performance";

        // Test that the internal payload is array-backed
        assert record.getPayload().hasArray() : "Record payload should be array-backed for zero-copy performance";

        // Test deserialized buffers are array-backed
        byte[] bytes = new byte[serializedBuffer.remaining()];
        serializedBuffer.get(bytes);
        ImprintRecord deserialized = ImprintRecord.deserialize(bytes);

        assert deserialized.getPayload().hasArray() : "Deserialized record payload should be array-backed";

        // Test that getRawBytes() returns array-backed buffers
        ByteBuffer rawBytes1 = deserialized.getRawBytes(1);
        ByteBuffer rawBytes2 = deserialized.getRawBytes(2);

        assert rawBytes1 != null && rawBytes1.hasArray() : "Raw bytes buffer for int field should be array-backed";
        assert rawBytes2 != null && rawBytes2.hasArray() : "Raw bytes buffer for string field should be array-backed";

        // Test complex types use array-backed buffers
        ImprintWriter complexWriter = new ImprintWriter(new SchemaId(2, 0xcafebabe));

        // Create array value
        List<Value> arrayValues = Arrays.asList(
            Value.fromInt32(1),
            Value.fromInt32(2),
            Value.fromInt32(3)
        );

        // Create map value
        Map<MapKey, Value> mapValues = new HashMap<>();
        mapValues.put(MapKey.fromString("key1"), Value.fromString("value1"));
        mapValues.put(MapKey.fromString("key2"), Value.fromString("value2"));

        complexWriter.addField(1, Value.fromArray(arrayValues))
                     .addField(2, Value.fromMap(mapValues));

        ImprintRecord complexRecord = complexWriter.build();

        assert complexRecord.getPayload().hasArray() : "Record with complex types should use array-backed payload";

        ByteBuffer complexSerialized = complexRecord.serializeToBuffer();
        assert complexSerialized.hasArray() : "Serialized buffer with complex types should be array-backed";

        // Test nested records use array-backed buffers
        ImprintWriter innerWriter = new ImprintWriter(new SchemaId(3, 0x12345678));
        innerWriter.addField(1, Value.fromString("nested data"));
        ImprintRecord innerRecord = innerWriter.build();

        ImprintWriter outerWriter = new ImprintWriter(new SchemaId(4, 0x87654321));
        outerWriter.addField(1, Value.fromRow(innerRecord));
        ImprintRecord outerRecord = outerWriter.build();

        assert innerRecord.getPayload().hasArray() : "Inner record payload should be array-backed";
        assert outerRecord.getPayload().hasArray() : "Outer record payload should be array-backed";

        ByteBuffer nestedSerialized = outerRecord.serializeToBuffer();
        assert nestedSerialized.hasArray() : "Serialized nested record should be array-backed";

        // Test builder pattern uses array-backed buffers
        ImprintRecord builderRecord = ImprintRecord.builder(1, 0xabcdef00)
            .field(1, "test string")
            .field(2, 42)
            .field(3, new byte[]{1, 2, 3})
            .build();

        assert builderRecord.getPayload().hasArray() : "Builder-created record should use array-backed payload";

        ByteBuffer builderSerialized = builderRecord.serializeToBuffer();
        assert builderSerialized.hasArray() : "Builder-created serialized buffer should be array-backed";

        System.out.println("✓ Array-backed buffers test passed");
    }
}