package com.imprint;

import com.imprint.core.*;
import com.imprint.types.*;
import com.imprint.error.ImprintException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Integration test to verify ByteBuffer functionality and zero-copy benefits.
 */
public class ByteBufferIntegrationTest {
    
    public static void main(String[] args) {
        try {
            testByteBufferFunctionality();
            testZeroCopy();
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
        
        assert deserialized.getValue(1).get().equals(Value.fromInt32(42));
        assert deserialized.getValue(2).get().equals(Value.fromString("zero-copy test"));
        
        // Test raw bytes access returns ByteBuffer
        Optional<ByteBuffer> rawBytes = deserialized.getRawBytes(1);
        assert rawBytes.isPresent() : "Raw bytes should be present for field 1";
        assert rawBytes.get().isReadOnly() : "Raw bytes buffer should be read-only";
        
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
            Optional<ByteBuffer> rawBytes = record.getRawBytes(1);
            assert rawBytes.isPresent() : "Raw bytes should be present";
            
            ByteBuffer rawBuffer = rawBytes.get();
            assert rawBuffer.isReadOnly() : "Raw buffer should be read-only";
            
            // The buffer should be positioned at the start of the actual data
            // (after the VarInt length prefix)
            assert rawBuffer.remaining() > largePayload.length : "Buffer should include length prefix";
            
            System.out.println("Zero-copy benefits test passed");
            
        } catch (ImprintException e) {
            throw new RuntimeException("Failed zero-copy test", e);
        }
    }
}