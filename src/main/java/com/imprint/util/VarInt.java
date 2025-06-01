package com.imprint.util;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import java.nio.ByteBuffer;

/**
 * Utility class for encoding and decoding variable-length integers (VarInt).
 * Supports encoding/decoding of 32-bit unsigned integers.
 */
public final class VarInt {
    
    private static final byte CONTINUATION_BIT = (byte) 0x80;
    private static final byte SEGMENT_BITS = 0x7f;
    private static final int MAX_VARINT_LEN = 5; // Enough for u32
    
    private VarInt() {} // utility class
    
    
    /**
     * Encode a 32-bit unsigned integer as a VarInt into the given ByteBuffer.
     * @param value the value to encode (treated as unsigned)
     * @param buffer the buffer to write to
     */
    public static void encode(int value, ByteBuffer buffer) {
        // Convert to unsigned long for proper bit manipulation
        long val = Integer.toUnsignedLong(value);
        
        // Encode at least one byte, then continue while value has more bits
        do {
            byte b = (byte) (val & SEGMENT_BITS);
            val >>>= 7;
            if (val != 0) {
                b |= CONTINUATION_BIT;
            }
            buffer.put(b);
        } while (val != 0);
    }
    
    
    /**
     * Decode a VarInt from a ByteBuffer.
     * @param buffer the buffer to decode from
     * @return a DecodeResult containing the decoded value and number of bytes consumed
     * @throws ImprintException if the VarInt is malformed
     */
    public static DecodeResult decode(ByteBuffer buffer) throws ImprintException {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;
        
        while (true) {
            if (bytesRead >= MAX_VARINT_LEN) {
                throw new ImprintException(ErrorType.MALFORMED_VARINT, "VarInt too long");
            }
            if (!buffer.hasRemaining()) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, 
                    "Unexpected end of data while reading VarInt");
            }
            
            byte b = buffer.get();
            bytesRead++;
            
            // Check if adding these 7 bits would overflow
            long segment = b & SEGMENT_BITS;
            if (shift >= 32 || (shift == 28 && segment > 0xF)) {
                throw new ImprintException(ErrorType.MALFORMED_VARINT, "VarInt overflow");
            }
            
            // Add the bottom 7 bits to the result
            result |= segment << shift;
            
            // If the high bit is not set, this is the last byte
            if ((b & CONTINUATION_BIT) == 0) {
                break;
            }
            
            shift += 7;
        }
        
        return new DecodeResult((int) result, bytesRead);
    }
    
    /**
     * Calculate the number of bytes needed to encode the given value as a VarInt.
     * @param value the value to encode (treated as unsigned)
     * @return the number of bytes needed
     */
    public static int encodedLength(int value) {
        // Convert to unsigned long for proper bit manipulation
        long val = Integer.toUnsignedLong(value);
        int length = 1;
        
        // Count additional bytes needed for values >= 128
        while (val >= 0x80) {
            val >>>= 7;
            length++;
        }
        
        return length;
    }
    
    /**
     * Result of a VarInt decode operation.
     */
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class DecodeResult {
        private final int value;
        private final int bytesRead;
    }
}