package com.imprint.util;

import lombok.Getter;
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
        System.getProperty("imprint.buffer.bounds.check", "false"));
    
    private byte[] array;
    private long baseOffset;
    private int capacity;
    private int position;
    private int limit;

    @Getter
    private final boolean growable;
    
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
        this(array, false);
    }
    
    /**
     * Create buffer wrapping a byte array with optional growth capability.
     */
    public ImprintBuffer(byte[] array, boolean growable) {
        // Inline the common case to avoid constructor delegation overhead
        if (BOUNDS_CHECK && array == null) {
            throw new IllegalArgumentException("Array cannot be null");
        }
        
        this.array = array;
        this.baseOffset = ARRAY_BYTE_BASE_OFFSET;
        this.capacity = array.length;
        this.position = 0;
        this.limit = array.length;
        this.growable = growable;
    }
    
    /**
     * Create buffer wrapping a portion of byte array.
     */
    public ImprintBuffer(byte[] array, int offset, int length) {
        this(array, offset, length, false);
    }
    
    /**
     * Create buffer wrapping a portion of byte array with optional growth capability.
     */
    public ImprintBuffer(byte[] array, int offset, int length, boolean growable) {
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
        this.growable = growable;
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
     * Create growable buffer with initial capacity.
     * Uses Agrona-style growth strategy for maximum performance.
     */
    public static ImprintBuffer growable(int initialCapacity) {
        return new ImprintBuffer(new byte[initialCapacity], true);
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
     * Ensure buffer has space for additional bytes.
     */
    private void ensureCapacity(int additionalBytes) {
        if (!growable) return;
        
        int requiredCapacity = position + additionalBytes;
        if (requiredCapacity <= capacity) return;
        
        // Agrona-style growth: 1.5x with 64-byte alignment for cache efficiency
        int newCapacity = Math.max(requiredCapacity, (capacity * 3) / 2);
        newCapacity = (newCapacity + 63) & ~63; // Align to 64-byte boundary
        
        // Allocate new array and copy existing data
        byte[] newArray = new byte[newCapacity];
        int currentOffset = (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET);
        
        // Use UNSAFE for ultra-fast bulk copy
        UNSAFE.copyMemory(array, baseOffset, newArray, ARRAY_BYTE_BASE_OFFSET + currentOffset, position);
        
        // Update buffer state
        this.array = newArray;
        this.baseOffset = ARRAY_BYTE_BASE_OFFSET + currentOffset;
        this.capacity = newCapacity - currentOffset;
        this.limit = capacity;
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
        ensureCapacity(1);
        if (BOUNDS_CHECK && !growable && position >= limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        UNSAFE.putByte(array, baseOffset + position, value);
        position++;
        return this;
    }

    /**
     * Write single byte.
     */
    public ImprintBuffer putUnsafeByte(byte value) {
        UNSAFE.putByte(array, baseOffset + position, value);
        position++;
        return this;
    }
    
    /**
     * Write short in little-endian format.
     */
    public ImprintBuffer putShort(short value) {
        ensureCapacity(2);
        if (BOUNDS_CHECK && !growable && position + 2 > limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putShort(array, baseOffset + position, value);
        position += 2;
        return this;
    }

    public ImprintBuffer putUnsafeShort(short value) {
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putShort(array, baseOffset + position, value);
        position += 2;
        return this;
    }
    
    /**
     * Write int32 in little-endian format.
     */
    public ImprintBuffer putInt(int value) {
        ensureCapacity(4);
        if (BOUNDS_CHECK && !growable && position + 4 > limit) {
            throw new RuntimeException("Buffer overflow");
        }
        
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putInt(array, baseOffset + position, value);
        position += 4;
        return this;
    }

    /**
     * Write int32 in little-endian format.
     */
    public ImprintBuffer putUnsafeInt(int value) {
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putInt(array, baseOffset + position, value);
        position += 4;
        return this;
    }

    /**
     * Write int64 in little-endian format.
     */
    public ImprintBuffer putUnsafeLong(long value) {
        // Write directly - Unsafe uses native byte order, which is little-endian on x86
        UNSAFE.putLong(array, baseOffset + position, value);
        position += 8;
        return this;
    }

    /**
     * Write int64 in little-endian format.
     */
    public ImprintBuffer putLong(long value) {
        ensureCapacity(8);
        if (BOUNDS_CHECK && !growable && position + 8 > limit) {
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
        ensureCapacity(length);
        if (BOUNDS_CHECK) {
            if (!growable && position + length > limit) {
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

    public int getIntUnsafe(int position) {
        return UNSAFE.getInt(array, ARRAY_BYTE_BASE_OFFSET + arrayOffset() + position);
    }

    public short getShortUnsafe(int position) {
        return UNSAFE.getShort(array, ARRAY_BYTE_BASE_OFFSET + arrayOffset() + position);
    }

    public byte getByteUnsafe(int position) {
        return UNSAFE.getByte(array, ARRAY_BYTE_BASE_OFFSET + arrayOffset() + position);
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
        // Estimate max bytes needed (5 for int32 VarInt)
        ensureCapacity(5);
        
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
        // Estimate max bytes needed (10 for int64 VarLong)
        ensureCapacity(10);
        
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
     * Direct implementation for performance.
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
        
        // For growable buffers, use the actual array length to avoid capacity issues
        int bufferCapacity = growable ? (array.length - arrayOffset) : capacity;
        
        ByteBuffer buffer = ByteBuffer.wrap(array, arrayOffset, bufferCapacity);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(position);
        buffer.limit(Math.min(limit, bufferCapacity)); // Ensure limit doesn't exceed capacity
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
    
    /**
     * Ultra-fast memory move within the same buffer using UNSAFE operations.
     */
    public void moveMemory(int srcPos, int dstPos, int length) {
        if (length <= 0 || srcPos == dstPos) return;
        
        long srcAddress = baseOffset + srcPos;
        long dstAddress = baseOffset + dstPos;
        
        // Use UNSAFE copyMemory which handles overlapping regions optimally
        UNSAFE.copyMemory(array, srcAddress, array, dstAddress, length);
    }
    
    @Override
    public String toString() {
        return "ImprintBuffer{position=" + position + ", capacity=" + capacity + "}";
    }
}