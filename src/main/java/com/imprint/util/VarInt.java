package com.imprint.util;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import lombok.Value;
import lombok.experimental.UtilityClass;

/**
 * Utility class for encoding and decoding variable-length integers (VarInt).
 * Supports encoding/decoding of 32-bit unsigned integers.
 */
@UtilityClass
public final class VarInt {

    private static final byte CONTINUATION_BIT = (byte) 0x80;
    private static final byte SEGMENT_BITS = 0x7f;
    private static final int MAX_VARINT_LEN = 5; // Enough for u32

    // Simple cache for values 0-1023
    private static final int CACHE_SIZE = 1024;
    private static final int[] ENCODED_LENGTHS = new int[CACHE_SIZE];

    static {
        for (int i = 0; i < CACHE_SIZE; i++) {
            long val = Integer.toUnsignedLong(i);
            int length = 1;
            while (val >= 0x80) {
                val >>>= 7;
                length++;
            }
            ENCODED_LENGTHS[i] = length;
        }
    }

    /**
     * Encode a 32-bit unsigned integer as a VarInt into the given ByteBuffer.
     * @param value the value to encode (treated as unsigned)
     * @param buffer the buffer to write to
     */
    public static void encode(int value, ImprintBuffer buffer) {
        long val = Integer.toUnsignedLong(value);
        do {
            byte b = (byte) (val & SEGMENT_BITS);
            val >>>= 7;
            if (val != 0)
                b |= CONTINUATION_BIT;
            buffer.putByte(b);
        } while (val != 0);
    }

    /**
     * Decode a VarInt from a ByteBuffer.
     * @param buffer the buffer to decode from
     * @return a DecodeResult containing the decoded value and number of bytes consumed
     * @throws ImprintException if the VarInt is malformed
     */
    public static DecodeResult decode(ImprintBuffer buffer) throws ImprintException {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;

        while (true) {
            if (bytesRead >= MAX_VARINT_LEN)
                throw new ImprintException(ErrorType.MALFORMED_VARINT, "VarInt too long");
            if (!buffer.hasRemaining())
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Unexpected end of data while reading VarInt");

            byte b = buffer.get();
            bytesRead++;

            // Check if adding these 7 bits would overflow
            long segment = b & SEGMENT_BITS;
            if (shift >= 32 || (shift == 28 && segment > 0xF))
                throw new ImprintException(ErrorType.MALFORMED_VARINT, "VarInt overflow");
            // Add the bottom 7 bits to the result
            result |= segment << shift;

            // If the high bit is not set, this is the last byte
            if ((b & CONTINUATION_BIT) == 0)
                break;
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
        if (value >= 0 && value < CACHE_SIZE)
            return ENCODED_LENGTHS[value];
        long val = Integer.toUnsignedLong(value);
        int length = 1;
        while (val >= 0x80) {
            val >>>= 7;
            length++;
        }
        return length;
    }

    /**
     * Result of a VarInt decode operation.
     */
    @Value
    public static class DecodeResult {
        int value;
        int bytesRead;
    }
}