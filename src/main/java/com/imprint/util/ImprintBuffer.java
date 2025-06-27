package com.imprint.util;

import com.imprint.error.ImprintException;
import lombok.Getter;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public final class ImprintBuffer {
    
    // Bounds checking control
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
     * Create growable buffer with initial capacity.
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
        
        // Buffer growth strategy: 1.5x with 64-byte alignment - should help with cache efficiency
        int newCapacity = Math.max(requiredCapacity, (capacity * 3) / 2);
        newCapacity = (newCapacity + 63) & ~63; // Align to 64-byte boundary
        
        // Allocate new array and copy existing data
        byte[] newArray = new byte[newCapacity];
        int currentOffset = (int) (baseOffset - ARRAY_BYTE_BASE_OFFSET);

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
     * Create read-only view (returns this since ImprintBuffer is write-focused anyways).
     */
    public ImprintBuffer asReadOnlyBuffer() {
        return duplicate();
    }
    
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
        
        // Write directly - Unsafe already uses native byte order, which is little-endian on x86
        UNSAFE.putShort(array, baseOffset + position, value);
        position += 2;
        return this;
    }

    public ImprintBuffer putUnsafeShort(short value) {
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
        UNSAFE.putInt(array, baseOffset + position, value);
        position += 4;
        return this;
    }

    /**
     * Write int32 in little-endian format.
     */
    public ImprintBuffer putUnsafeInt(int value) {
        UNSAFE.putInt(array, baseOffset + position, value);
        position += 4;
        return this;
    }

    /**
     * Write int64 in little-endian format.
     */
    public ImprintBuffer putUnsafeLong(long value) {
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

    
    /**
     * Write VarInt using the centralized VarInt utility class.
     */
    public ImprintBuffer putVarInt(int value) {
        VarInt.encode(value, this);
        return this;
    }
    
    /**
     * Read VarInt using the centralized VarInt utility class.
     */
    public int getVarInt() {
        try {
            return VarInt.decode(this).getValue();
        } catch (ImprintException e) {
            throw new RuntimeException("Failed to decode VarInt", e);
        }
    }
    
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

    public void moveMemory(int srcPos, int dstPos, int length) {
        if (length <= 0 || srcPos == dstPos) return;
        
        long srcAddress = baseOffset + srcPos;
        long dstAddress = baseOffset + dstPos;
        
        // Use UNSAFE copyMemory which handles overlapping regions optimally
        UNSAFE.copyMemory(array, srcAddress, array, dstAddress, length);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ImprintBuffer other = (ImprintBuffer) obj;
        
        // Quick checks first
        if (remaining() != other.remaining()) return false;
        
        // Save positions
        int thisPos = this.position;
        int otherPos = other.position;
        
        try {
            // Compare byte by byte from current positions
            while (hasRemaining() && other.hasRemaining()) {
                if (get() != other.get()) {
                    return false;
                }
            }
            return true;
        } finally {
            // Restore positions
            this.position = thisPos;
            other.position = otherPos;
        }
    }
    
    @Override
    public int hashCode() {
        int h = 1;
        int savedPos = position;
        try {
            while (hasRemaining()) {
                h = 31 * h + get();
            }
            return h;
        } finally {
            position = savedPos;
        }
    }
    
    // ========== BYTEBUFFER COMPATIBILITY METHODS ==========
    
    /**
     * Clear the buffer (set position to 0, limit to capacity).
     */
    public ImprintBuffer clear() {
        position = 0;
        limit = capacity;
        return this;
    }
    
    /**
     * Rewind the buffer (set position to 0).
     */
    public ImprintBuffer rewind() {
        position = 0;
        return this;
    }
    
    /**
     * Put a single byte (ByteBuffer compatibility).
     */
    public ImprintBuffer put(byte b) {
        return putByte(b);
    }
    
    /**
     * Put a byte array (ByteBuffer compatibility).
     */
    public ImprintBuffer put(byte[] src) {
        return putBytes(src);
    }
    
    /**
     * Put a portion of byte array (ByteBuffer compatibility).
     */
    public ImprintBuffer put(byte[] src, int offset, int length) {
        return putBytes(src, offset, length);
    }
    
    @Override
    public String toString() {
        return "ImprintBuffer{position=" + position + ", capacity=" + capacity + "}";
    }
}