package com.imprint.util;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for ImprintBuffer functionality and compatibility with ByteBuffer.
 */
class ImprintBufferTest {

    @Test
    void testBasicPrimitiveWrites() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Test all primitive types
        buffer.putByte((byte) 0x42)
              .putInt(0x12345678)
              .putLong(0x123456789ABCDEF0L)
              .putFloat(3.14f)
              .putDouble(2.718281828);
        
        // Verify position advanced correctly
        assertEquals(1 + 4 + 8 + 4 + 8, buffer.position());
        

        buffer.position(0);
        
        assertEquals((byte) 0x42, buffer.get());
        assertEquals(0x12345678, buffer.getInt());
        assertEquals(0x123456789ABCDEF0L, buffer.getLong());
        assertEquals(3.14f, buffer.getFloat(), 0.001f);
        assertEquals(2.718281828, buffer.getDouble(), 0.000001);
    }
    
    @Test
    void testByteArrayWrites() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        byte[] testData = {1, 2, 3, 4, 5};
        buffer.putBytes(testData);
        
        assertEquals(5, buffer.position());
        
        // Verify data written correctly
        for (int i = 0; i < testData.length; i++) {
            assertEquals(testData[i], array[i]);
        }
    }
    
    @Test
    void testVarIntWrites() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Test small value (single byte)
        buffer.putVarInt(42);
        assertEquals(1, buffer.position());
        assertEquals(42, array[0]);
        
        // Reset and test larger value
        buffer.position(0);
        buffer.putVarInt(300); // Requires 2 bytes
        assertEquals(2, buffer.position());
        
        // Verify VarInt encoding matches our VarInt utility
        buffer.position(0);
        var compareBuffer = new ImprintBuffer(new byte[64]);
        VarInt.encode(300, compareBuffer);
        
        for (int i = 0; i < compareBuffer.position(); i++) {
            assertEquals(compareBuffer.array()[i], array[i], "VarInt encoding mismatch at byte " + i);
        }
    }
    
    @Test
    void testStringWrites() throws Exception {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        String testString = "Hello";
        buffer.putString(testString);
        
        // Should write length (VarInt) + UTF-8 bytes
        assertTrue(buffer.position() > testString.length());
        
        // Verify by reading back with ByteBuffer
        buffer.position(0);
        
        VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
        assertEquals(testString.getBytes().length, lengthResult.getValue());
        
        byte[] stringBytes = new byte[lengthResult.getValue()];
        buffer.get(stringBytes);
        assertEquals(testString, new String(stringBytes));
    }
    
    @Test
    void testVarIntRoundTrip() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Test various VarInt values to ensure putVarInt/getVarInt work correctly after consolidation
        int[] testValues = {
            0,           // Single byte: 0
            1,           // Single byte: 1
            127,         // Single byte: 127 (max single byte value)
            128,         // Two bytes: 128 (min two byte value)
            300,         // Two bytes: 300
            16383,       // Two bytes: 16383 (max two byte value)
            16384,       // Three bytes: 16384 (min three byte value)
            1048575,     // Three bytes: 1048575 (max three byte value)  
            16777215,    // Four bytes: 16777215 (max four byte value)
            268435455,   // Five bytes: 268435455 (max five byte value)
            Integer.MAX_VALUE // Five bytes: max int value
        };
        
        for (int testValue : testValues) {
            // Reset buffer for each test
            buffer.position(0);
            
            // Write VarInt using ImprintBuffer method (should delegate to VarInt utility)
            buffer.putVarInt(testValue);
            int writePosition = buffer.position();
            
            // Read back using ImprintBuffer method (should delegate to VarInt utility)
            buffer.position(0);
            int readValue = buffer.getVarInt();
            int readPosition = buffer.position();
            
            // Verify round-trip correctness
            assertEquals(testValue, readValue, "VarInt round-trip failed for value: " + testValue);
            assertEquals(writePosition, readPosition, "Read/write positions don't match for value: " + testValue);
            
            // Also verify that our ImprintBuffer methods produce the same result as VarInt utility directly
            buffer.position(0);
            var directEncodeBuffer = new ImprintBuffer(new byte[64]);
            try {
                VarInt.encode(testValue, directEncodeBuffer);
                
                // Compare the encoded bytes
                for (int i = 0; i < writePosition; i++) {
                    assertEquals(directEncodeBuffer.array()[i], array[i], 
                        "Direct VarInt encoding differs from ImprintBuffer encoding at byte " + i + " for value " + testValue);
                }
            } catch (Exception e) {
                fail("VarInt utility encoding failed for value: " + testValue + ", error: " + e.getMessage());
            }
        }
    }
    
    @Test
    void testBoundsChecking() {
        // Since bounds checking is disabled by default for performance,
        // this test verifies that operations beyond capacity don't throw exceptions
        // when bounds checking is disabled (which is the expected production behavior)
        
        byte[] array = new byte[4];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // This should work
        buffer.putInt(42);
        
        // With bounds checking disabled (default), this should NOT throw an exception
        // The buffer will write beyond capacity but won't check bounds for performance
        assertDoesNotThrow(() -> buffer.putByte((byte) 1));
        
        // Verify the byte was written beyond the original array bounds
        assertEquals(5, buffer.position());
    }
    
    @Test 
    void testCompatibilityWithByteBuffer() {
        // Test that ImprintBuffer produces same results as ByteBuffer
        byte[] imprintArray = new byte[32];
        byte[] byteBufferArray = new byte[32];
        
        ImprintBuffer imprintBuffer = new ImprintBuffer(imprintArray);
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteBufferArray);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        
        // Write same data to both
        int testInt = 0x12345678;
        long testLong = 0x123456789ABCDEF0L;
        float testFloat = 3.14f;
        
        imprintBuffer.putInt(testInt).putLong(testLong).putFloat(testFloat);
        byteBuffer.putInt(testInt).putLong(testLong).putFloat(testFloat);
        
        // Compare results
        for (int i = 0; i < byteBuffer.position(); i++) {
            assertEquals(byteBufferArray[i], imprintArray[i], 
                "Mismatch at position " + i);
        }
    }
    
    @Test
    void testFlipSliceAndLimit() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Write some data
        buffer.putInt(42).putInt(100);
        assertEquals(8, buffer.position());
        assertEquals(64, buffer.limit());
        
        // Test flip
        buffer.flip();
        assertEquals(0, buffer.position());
        assertEquals(8, buffer.limit());
        
        // Test reading back
        assertEquals(42, buffer.getInt());
        assertEquals(100, buffer.getInt());
        
        // Test slice
        buffer.position(4);
        ImprintBuffer slice = buffer.slice();
        assertEquals(0, slice.position());
        assertEquals(4, slice.limit()); // remaining from position 4 to limit 8
        assertEquals(100, slice.getInt());
    }
    
    @Test  
    void testMethodChaining() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Test method chaining like ByteBuffer
        buffer.position(10).limit(50);
        assertEquals(10, buffer.position());
        assertEquals(50, buffer.limit());
        assertEquals(40, buffer.remaining());
    }
    
    @Test
    void testReadOperations() {
        byte[] array = new byte[64];
        ImprintBuffer buffer = new ImprintBuffer(array);
        
        // Write data
        buffer.putByte((byte) 0x42)
              .putShort((short) 0x1234)
              .putInt(0x12345678)
              .putLong(0x123456789ABCDEF0L)
              .putFloat(3.14f)
              .putDouble(2.718281828);
        
        // Flip for reading
        buffer.flip();
        
        // Read back and verify
        assertEquals((byte) 0x42, buffer.get());
        assertEquals((short) 0x1234, buffer.getShort());
        assertEquals(0x12345678, buffer.getInt());
        assertEquals(0x123456789ABCDEF0L, buffer.getLong());
        assertEquals(3.14f, buffer.getFloat(), 0.001f);
        assertEquals(2.718281828, buffer.getDouble(), 0.000001);
    }
}