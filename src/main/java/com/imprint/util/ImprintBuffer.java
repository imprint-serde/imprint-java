package com.imprint.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * High-performance buffer optimized specifically for Imprint serialization.
 * Eliminates ByteBuffer overhead by using direct memory access via Unsafe.
 * <p>
 * Key optimizations:
 * - No bounds checking (disabled via system property)
 * - Direct memory writes without position tracking
 * - Little-endian optimized for x86
 * - Minimal API surface for serialization use case
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public final class ImprintBuffer {
    
    // Bounds checking control - can be disabled for production
    private static final boolean BOUNDS_CHECK = Boolean.parseBoolean(
        System.getProperty("imprint.buffer.bounds.check", "true"));
    
    private final byte[] array;
    private final long baseOffset;
    private final int capacity;
    private int position;
    private int limit;
    
    // Unsafe instance for direct memory access
    private static final Unsafe UNSAFE;
    private static final long ARRAY_BYTE_BASE_OFFSET;
    
    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            ARRAY_BYTE_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access Unsafe", e);
        }
    }
    
    /**
     * Create buffer wrapping a byte array.
     */
    public ImprintBuffer(byte[] array) {
        this(array, 0, array.length);
    }
    
    /**
     * Create buffer wrapping a portion of byte array.
     */
    public ImprintBuffer(byte[] array, int offset, int length) {
        if (BOUNDS_CHECK) {
            if (offset < 0 || length < 0 || offset + length > array.length) {
                throw new IllegalArgumentException("Invalid offset/length");
            }
        }
        
        this.array = array;
        this.baseOffset = ARRAY_BYTE_BASE_OFFSET + offset;
        this.capacity = length;
        this.position = 0;
        this.limit = length;
    }
    
    /**
     * Create buffer from ByteBuffer (for compatibility).
     */
    public static ImprintBuffer wrap(ByteBuffer byteBuffer) {
        if (byteBuffer.isDirect()) {
            throw new IllegalArgumentException("Direct ByteBuffers not yet supported");
        }
        
        byte[] array = byteBuffer.array();
        int offset = byteBuffer.arrayOffset() + byteBuffer.position();
        int length = byteBuffer.remaining();
        
        ImprintBuffer buffer = new ImprintBuffer(array, offset, length);
        // Preserve the ByteBuffer's position and limit state
        buffer.position = 0;
        buffer.limit = length;
        return buffer;
    }
    
    /**
     * Create read-only buffer from ByteBuffer data (copying the data).
     */
    public static ImprintBuffer fromByteBuffer(ByteBuffer byteBuffer) {
        byte[] data = new byte[byteBuffer.remaining()];
        byteBuffer.duplicate().get(data);
        return new ImprintBuffer(data);
    }
    
    /**
     * Get current position.
     */
    public int position() {
        return position;
    }
    
    /**
     * Set position.
     */
    public ImprintBuffer position(int newPosition) {
        if (BOUNDS_CHECK && (newPosition < 0 || newPosition > limit)) {
            throw new IllegalArgumentException("Invalid position: " + newPosition);
        }
        this.position = newPosition;
        return this;
    }
    
    /**
     * Get limit.
     */
    public int limit() {
        return limit;
    }
    
    /**
     * Set limit.
     */
    public ImprintBuffer limit(int newLimit) {
        if (BOUNDS_CHECK && (newLimit < 0 || newLimit > capacity)) {
            throw new IllegalArgumentException("Invalid limit: " + newLimit);
        }
        this.limit = newLimit;
        return this;
    }
    
    /**
     * Get remaining bytes.
     */
    public int remaining() {
        return limit - position;
    }
    
    /**
     * Check if buffer has remaining bytes.
     */
    public boolean hasRemaining() {
        return position < limit;
    }
    
    /**
     * Get capacity.
     */
    public int capacity() {
        return capacity;
    }
    
    /**
     * Flip buffer for reading (set limit to position, position to 0).
     */
    public ImprintBuffer flip() {
        limit = position;
        position = 0;
        return this;
    }
    
    /**
     * Create a slice of this buffer starting from current position.
     */
    public ImprintBuffer slice() {
        int offset = (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET) + position;
        return new ImprintBuffer(array, offset, remaining());
    }
    
    /**
     * Create a duplicate of this buffer.
     */
    public ImprintBuffer duplicate() {
        int offset = (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET);
        var dup = new ImprintBuffer(array, offset, capacity);
        dup.position = this.position;
        dup.limit = this.limit;
        return dup;
    }
    
    /**
     * Create read-only view (returns this since ImprintBuffer is write-focused).
     */
    public ImprintBuffer asReadOnlyBuffer() {
        return duplicate();
    }
    
    // ========== PRIMITIVE WRITES (Little-Endian Optimized) ==========
    
    /**
     * Write single byte.
     */
    public ImprintBuffer putByte(byte value) {
        if (BOUNDS_CHECK && position >= limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        UNSAFE.putByte(array, baseOffset + position, value);
        position++;
        return this;
    }
    
    /**
     * Write short in little-endian format.
     */
    public ImprintBuffer putShort(short value) {
        if (BOUNDS_CHECK && position + 2 > limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putShort(array, baseOffset + position, value);
        position += 2;
        return this;
    }
    
    /**
     * Write int32 in little-endian format.
     */
    public ImprintBuffer putInt(int value) {
        if (BOUNDS_CHECK && position + 4 > limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putInt(array, baseOffset + position, value);
        position += 4;
        return this;
    }
    
    /**
     * Write int64 in little-endian format.
     */
    public ImprintBuffer putLong(long value) {
        if (BOUNDS_CHECK && position + 8 > limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putLong(array, baseOffset + position, value);
        position += 8;
        return this;
    }
    
    /**
     * Write float32 in little-endian format.
     */
    public ImprintBuffer putFloat(float value) {
        return putInt(Float.floatToRawIntBits(value));
    }
    
    /**
     * Write float64 in little-endian format.
     */
    public ImprintBuffer putDouble(double value) {
        return putLong(Double.doubleToRawLongBits(value));
    }
    
    /**
     * Write byte array.
     */
    public ImprintBuffer putBytes(byte[] src) {
        return putBytes(src, 0, src.length);
    }
    
    /**
     * Write portion of byte array.
     */
    public ImprintBuffer putBytes(byte[] src, int srcOffset, int length) {
        if (BOUNDS_CHECK) {
            if (position + length > limit) {
                throw new RuntimeException("Buffer overflow");
            }
            if (srcOffset < 0 || length < 0 || srcOffset + length > src.length) {
                throw new IllegalArgumentException("Invalid src parameters");
            }
        }
        
        // Bulk copy via Unsafe
        UNSAFE.copyMemory(src, ARRAY_BYTE_BASE_OFFSET + srcOffset,
                         array, baseOffset + position, length);
        position += length;
        return this;
    }
    
    // ========== PRIMITIVE READS (Little-Endian Optimized) ==========
    
    /**
     * Read single byte.
     */
    public byte get() {
        if (BOUNDS_CHECK && position >= limit) {
            throw new RuntimeException("Buffer underflow");
        }
        
        byte value = UNSAFE.getByte(array, baseOffset + position);
        position++;
        return value;
    }
    
    /**
     * Read short in little-endian format.
     */
    public short getShort() {
        if (BOUNDS_CHECK && position + 2 > limit) {
            throw new RuntimeException("Buffer underflow");
        }
        
        short value = UNSAFE.getShort(array, baseOffset + position);
        position += 2;
        return value;
    }
    
    /**
     * Read int32 in little-endian format.
     */
    public int getInt() {
        if (BOUNDS_CHECK && position + 4 > limit) {
            throw new RuntimeException("Buffer underflow");
        }
        
        int value = UNSAFE.getInt(array, baseOffset + position);
        position += 4;
        return value;
    }
    
    /**
     * Read int64 in little-endian format.
     */
    public long getLong() {
        if (BOUNDS_CHECK && position + 8 > limit) {
            throw new RuntimeException("Buffer underflow");
        }
        
        long value = UNSAFE.getLong(array, baseOffset + position);
        position += 8;
        return value;
    }
    
    /**
     * Read float32 in little-endian format.
     */
    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }
    
    /**
     * Read float64 in little-endian format.
     */
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }
    
    /**
     * Read bytes into array.
     */
    public ImprintBuffer get(byte[] dst) {
        return get(dst, 0, dst.length);
    }
    
    /**
     * Read bytes into portion of array.
     */
    public ImprintBuffer get(byte[] dst, int dstOffset, int length) {
        if (BOUNDS_CHECK) {
            if (position + length > limit) {
                throw new RuntimeException("Buffer underflow");
            }
            if (dstOffset < 0 || length < 0 || dstOffset + length > dst.length) {
                throw new IllegalArgumentException("Invalid dst parameters");
            }
        }
        
        // Bulk copy via Unsafe
        UNSAFE.copyMemory(array, baseOffset + position,
                         dst, ARRAY_BYTE_BASE_OFFSET + dstOffset, length);
        position += length;
        return this;
    }
    
    // ========== OPTIMIZED VARINT WRITES ==========
    
    /**
     * Write VarInt directly to buffer (optimized).
     */
    public ImprintBuffer putVarInt(int value) {
        // Fast path for small values (most common case)
        if ((value & 0xFFFFFF80) == 0) {
            return putByte((byte) value);
        }
        
        // Unrolled loop for better performance
        while ((value & 0xFFFFFF80) != 0) {
            putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        return putByte((byte) value);
    }
    
    /**
     * Write VarLong directly to buffer (optimized).
     */
    public ImprintBuffer putVarLong(long value) {
        // Fast path for small values (most common case)
        if ((value & 0xFFFFFFFFFFFFFF80L) == 0) {
            return putByte((byte) value);
        }
        
        // Unrolled loop for better performance
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        return putByte((byte) value);
    }
    
    /**
     * Encode a VarInt into this buffer (compatible with VarInt.encode).
     */
    public void encodeVarInt(int value) {
        putVarInt(value);
    }
    
    /**
     * Read VarInt from buffer (optimized).
     */
    public int getVarInt() {
        int result = 0;
        int shift = 0;
        
        while (true) {
            if (BOUNDS_CHECK && position >= limit) {
                throw new RuntimeException("Buffer underflow reading VarInt");
            }
            
            byte b = UNSAFE.getByte(array, baseOffset + position);
            position++;
            
            result |= (b & 0x7F) << shift;
            
            if ((b & 0x80) == 0) {
                break;
            }
            
            shift += 7;
            if (shift >= 32) {
                throw new RuntimeException("VarInt too long");
            }
        }
        
        return result;
    }
    
    /**
     * Read VarLong from buffer (optimized).
     */
    public long getVarLong() {
        long result = 0;
        int shift = 0;
        
        while (true) {
            if (BOUNDS_CHECK && position >= limit) {
                throw new RuntimeException("Buffer underflow reading VarLong");
            }
            
            byte b = UNSAFE.getByte(array, baseOffset + position);
            position++;
            
            result |= (long)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0) {
                break;
            }
            
            shift += 7;
            if (shift >= 64) {
                throw new RuntimeException("VarLong too long");
            }
        }
        
        return result;
    }
    
    // ========== STRING WRITES (UTF-8) ==========
    
    /**
     * Write UTF-8 string with length prefix.
     * TODO: Will be optimized with SWAR for ASCII strings.
     */

    public ImprintBuffer putString(String str) {
        byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
        putVarInt(utf8Bytes.length);
        return putBytes(utf8Bytes);
    }
    
    /**
     * Read UTF-8 string with length prefix.
     */
    public String getString() {
        int length = getVarInt();
        byte[] utf8Bytes = new byte[length];
        get(utf8Bytes);
        return new String(utf8Bytes, StandardCharsets.UTF_8);
    }
    
    // ========== UTILITY METHODS ==========
    
    /**
     * Convert to ByteBuffer for compatibility.
     */
    public ByteBuffer toByteBuffer() {
        int arrayOffset = (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET);
        ByteBuffer buffer = ByteBuffer.wrap(array, arrayOffset, capacity);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(position);
        buffer.limit(limit);
        return buffer;
    }
    
    /**
     * Get backing array (for zero-copy operations).
     */
    public byte[] array() {
        return array;
    }
    
    /**
     * Get array offset.
     */
    public int arrayOffset() {
        return (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET);
    }
    
    @Override
    public String toString() {
        return "ImprintBuffer{position=" + position + ", capacity=" + capacity + "}";
    }
}