package com.imprint.core;

import lombok.Value;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Specialized short→object map optimized for ImprintRecordBuilder field IDs.
 * Implementation
 * - No key-value boxing/unboxing
 * - Primitive int16 keys
 * - Open addressing with linear probing
 * - Sacrifices map to sort values in place and return without allocation/copy
 */
final class ImprintFieldObjectMap<T> {
    private static final int DEFAULT_CAPACITY = 64;
    private static final float LOAD_FACTOR = 0.75f;
    private static final short EMPTY_KEY = -1; // Reserved empty marker (field IDs are >= 0)
    
    private short[] keys;
    private Object[] values;
    private int size;
    private int threshold;
    //sorting in place and returning the map's internal references means we don't have to allocate or copy to a new array;
    //this is definitely a suspicious pattern at best though
    private boolean poisoned = false;
    
    public ImprintFieldObjectMap() {
        this(DEFAULT_CAPACITY);
    }
    
    public ImprintFieldObjectMap(int initialCapacity) {
        int capacity = nextPowerOfTwo(Math.max(4, initialCapacity));
        this.keys = new short[capacity];
        this.values = new Object[capacity];
        this.threshold = (int) (capacity * LOAD_FACTOR);
        Arrays.fill(keys, EMPTY_KEY);
    }

    public void put(short key, T value)  {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        putValue(key, value);
    }

    public void put(int key, T value) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key > Short.MAX_VALUE)
            throw new IllegalArgumentException("Field ID must be 0-" + Short.MAX_VALUE + ", got: " + key);
        putValue((short) key, value);
    }

    /**
     * Put a value and return the previous value if any.
     * @return the previous value, or null if no previous value existed
     */
    public T putAndReturnOld(int key, T value) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key > Short.MAX_VALUE)
            throw new IllegalArgumentException("Field ID must be 0-" + Short.MAX_VALUE + ", got: " + key);
        return putValueAndReturnOld((short) key, value);
    }

    private void  putValue(short key, T value) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key < 0)
            throw new IllegalArgumentException("Field ID must be 0-" + Short.MAX_VALUE + ", got: " + key);

        if (size >= threshold)
            resize();
        int index = findSlot(key);
        if (keys[index] == EMPTY_KEY) {
            size++;
        }
        keys[index] = key;
        values[index] = value;
    }

    @SuppressWarnings("unchecked")
    private T putValueAndReturnOld(short key, T value) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key < 0)
            throw new IllegalArgumentException("Field ID must be 0-" + Short.MAX_VALUE + ", got: " + key);

        if (size >= threshold)
            resize();
        int index = findSlot(key);
        T oldValue = null;
        if (keys[index] == EMPTY_KEY) {
            size++;
        } else {
            // Existing key - capture old value
            oldValue = (T) values[index];
        }
        keys[index] = key;
        values[index] = value;
        return oldValue;
    }
    
    @SuppressWarnings("unchecked")
    public T get(int key) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key < 0 || key > Short.MAX_VALUE)
            return null;
        short shortKey = (short) key;
        int index = findSlot(shortKey);
        return keys[index] == shortKey ? (T) values[index] : null;
    }
    
    public boolean containsKey(int key) {
        if (poisoned)
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        if (key < 0 || key > Short.MAX_VALUE) return false;
        short shortKey = (short) key;
        
        int index = findSlot(shortKey);
        return keys[index] == shortKey;
    }
    
    public int size() {
        return size;
    }
    
    public boolean isEmpty() {
        return size == 0;
    }
    
    /**
     * Get all keys (non-destructive).
     */
    public int[] getKeys() {
        return IntStream.range(0, keys.length)
                .filter(i -> keys[i] != EMPTY_KEY)
                .map(i -> keys[i]).toArray();
    }
    
    /**
     * Stream all keys without allocation.
     * Non-destructive operation that can be called multiple times.
     * 
     * @return IntStream of all keys in the map
     */
    public IntStream streamKeys() {
        if (poisoned) {
            throw new IllegalStateException("Map is invalid after compaction - cannot perform operations");
        }
        
        return IntStream.range(0, keys.length)
            .filter(i -> keys[i] != EMPTY_KEY)
            .map(i -> keys[i]);
    }
    
    /**
     * Result holder for in-place sorted values - avoids Array.copy allocations by returning
     * array reference and valid count.
     */
    public static final class SortedValuesResult {
        public final Object[] values;
        public final int count;

        SortedValuesResult(Object[] values, int count) {
            this.values = values;
            this.count = count;
        }
    }

    /**
     * Get values sorted by key order with zero allocation by left-side compacting the value set.
     * WARNING: Modifies internal state, and renders map operations unstable and in an illegal state. Only invoke this
     * if you plan to discard the map afterward.
     * (e.g., at the end of builder lifecycle before build()).
     *
     * @return SortedValuesResult containing the internal values array and valid count.
     *         Caller should iterate from 0 to result.count-1 only.
     */
    public SortedValuesResult getSortedValues() {
        if (size == 0) {
            // Poison the map even when empty, even if just for consistency
            poisoned = true;
            return new SortedValuesResult(values, 0);
        }
        
        // left side compaction of all entries to the front of the arrays
        compactEntries();
        
        // Sort the compacted entries by key in-place
        sortEntriesByKey(size);
        
        // Poison the map - no further operations allowed
        poisoned = true;
        
        // Return the internal array w/ count
        return new SortedValuesResult(values, size);
    }
    
    /**
     * Get values sorted by key order.
     * Does not modify internal state and can be invoked repeatedly.
     * 
     * @param resultArray Array to store results (will be resized if needed)
     * @return Sorted array of values
     */
    @SuppressWarnings("unchecked")
    public T[] getSortedValuesCopy(T[] resultArray) {
        if (poisoned)
            throw new IllegalStateException("Map is poisoned after destructive sort - cannot perform operations");
        if (size == 0)
            return resultArray.length == 0 ? resultArray : Arrays.copyOf(resultArray, 0);

        // Create temporary arrays for non-destructive sort
        var tempKeys = new short[size];
        var tempValues = new Object[size];
        
        // Copy valid entries to temporary arrays
        int writeIndex = 0;
        for (int readIndex = 0; readIndex < keys.length; readIndex++) {
            if (keys[readIndex] != EMPTY_KEY) {
                tempKeys[writeIndex] = keys[readIndex];
                tempValues[writeIndex] = values[readIndex];
                writeIndex++;
            }
        }
        
        // Sort the temporary arrays by key
        for (int i = 1; i < size; i++) {
            short key = tempKeys[i];
            Object value = tempValues[i];
            int j = i - 1;
            
            while (j >= 0 && tempKeys[j] > key) {
                tempKeys[j + 1] = tempKeys[j];
                tempValues[j + 1] = tempValues[j];
                j--;
            }
            
            tempKeys[j + 1] = key;
            tempValues[j + 1] = value;
        }
        
        // Copy sorted values to result array
        if (resultArray.length != size)
            resultArray = Arrays.copyOf(resultArray, size);
        
        for (int i = 0; i < size; i++)
            resultArray[i] = (T) tempValues[i];
        
        return resultArray;
    }
    
    /**
     * Left side compact for all non-empty entries to the front of keys/values arrays.
     */
    private void compactEntries() {
        int writeIndex = 0;
        
        for (int readIndex = 0; readIndex < keys.length; readIndex++) {
            if (keys[readIndex] != EMPTY_KEY) {
                if (writeIndex != readIndex) {
                    keys[writeIndex] = keys[readIndex];
                    values[writeIndex] = values[readIndex];
                    
                    // Clear the old slot
                    keys[readIndex] = EMPTY_KEY;
                    values[readIndex] = null;
                }
                writeIndex++;
            }
        }
    }
    
    /**
     * Sort the first 'count' entries by key using insertion sort (should be fast enough for small arrays).
     * //TODO some duplication in here with the sorted values copy
     */
    private void sortEntriesByKey(int count) {
        for (int i = 1; i < count; i++) {
            short key = keys[i];
            var value = values[i];
            int j = i - 1;
            
            while (j >= 0 && keys[j] > key) {
                keys[j + 1] = keys[j];
                values[j + 1] = values[j];
                j--;
            }
            
            keys[j + 1] = key;
            values[j + 1] = value;
        }
    }
    
    
    private int findSlot(short key) {
        int mask = keys.length - 1;
        int index = hash(key) & mask;
        
        // Linear probing
        while (keys[index] != EMPTY_KEY && keys[index] != key) {
            index = (index + 1) & mask;
        }
        
        return index;
    }
    
    private void resize() {
        short[] oldKeys = keys;
        Object[] oldValues = values;
        
        int newCapacity = keys.length * 2;
        keys = new short[newCapacity];
        values = new Object[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);
        Arrays.fill(keys, EMPTY_KEY);
        
        int oldSize = size;
        size = 0;

        for (int i = 0; i < oldKeys.length; i++) {
            if (oldKeys[i] != EMPTY_KEY) {
                @SuppressWarnings("unchecked")
                T value = (T) oldValues[i];
                put(oldKeys[i], value);
            }
        }
        //TODO remove this assertion (carried from from EclipseCollection)
        assert size == oldSize;
    }

    private static int hash(short key) {
        int intKey = key & 0xFFFF;
        intKey ^= intKey >>> 8;
        return intKey;
    }
    
    private static int nextPowerOfTwo(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    /**
     * Result holder for in-place sorted fields - returns both keys and values.
     */
    @Value
    public static class SortedFieldsResult {
        short[] keys;
        Object[] values;
        int count;
    }

    /**
     * Get both keys and values sorted by key order with zero allocation.
     * WARNING: Modifies internal state, and renders map operations unstable and in an illegal state.
     */
    public SortedFieldsResult getSortedFields() {
        //It makes more sense to poison the map here for consistency, even though technically it isn't with 0 fields.
        if (size == 0) {
            poisoned = true;
            return new SortedFieldsResult(keys, values, 0);
        }
        
        compactEntries();
        sortEntriesByKey(size);
        poisoned = true;
        return new SortedFieldsResult(keys, values, size);
    }
}
