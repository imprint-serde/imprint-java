package com.imprint.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for IntObjectMap - specialized short→object map optimized for field IDs.
 */
class ImprintFieldObjectMapTest {
    
    private ImprintFieldObjectMap<String> map;
    
    @BeforeEach
    void setUp() {
        map = new ImprintFieldObjectMap<>();
    }
    
    @Test
    void shouldPutAndGetBasicOperations() {
        map.put(1, "one");
        map.put(5, "five");
        map.put(10, "ten");
        
        assertEquals("one", map.get(1));
        assertEquals("five", map.get(5));
        assertEquals("ten", map.get(10));
        assertNull(map.get(99));
        assertEquals(3, map.size());
    }
    
    @Test
    void shouldHandleKeyValidation() {
        // Valid keys (0 to Short.MAX_VALUE)
        map.put(0, "zero");
        map.put(Short.MAX_VALUE, "max");
        
        // Invalid keys
        assertThrows(IllegalArgumentException.class, () -> map.put(-1, "negative"));
        assertThrows(IllegalArgumentException.class, () -> map.put(Short.MAX_VALUE + 1, "too_large"));
    }
    
    @Test
    void shouldHandleContainsKey() {
        map.put(1, "one");
        map.put(5, "five");
        
        assertTrue(map.containsKey(1));
        assertTrue(map.containsKey(5));
        assertFalse(map.containsKey(99));
        assertFalse(map.containsKey(-1));
        assertFalse(map.containsKey(Short.MAX_VALUE + 1));
    }
    
    @Test
    void shouldOverwriteExistingKeys() {
        map.put(1, "original");
        assertEquals("original", map.get(1));
        assertEquals(1, map.size());
        
        map.put(1, "updated");
        assertEquals("updated", map.get(1));
        assertEquals(1, map.size()); // Size should not increase
    }
    
    @Test
    void shouldGetKeysArray() {
        map.put(3, "three");
        map.put(1, "one");
        map.put(7, "seven");
        
        int[] keys = map.getKeys();
        assertEquals(3, keys.length);
        
        // Convert to set for order-independent comparison
        var keySet = java.util.Arrays.stream(keys).boxed()
            .collect(java.util.stream.Collectors.toSet());
        
        assertTrue(keySet.contains(1));
        assertTrue(keySet.contains(3));
        assertTrue(keySet.contains(7));
    }
    
    @Test
    void shouldSortValuesNonDestructively() {
        map.put(3, "three");
        map.put(1, "one");
        map.put(7, "seven");
        map.put(2, "two");
        
        // Test non-destructive sort
        String[] sorted = map.getSortedValuesCopy(new String[0]);
        
        assertEquals(4, sorted.length);
        assertEquals("one", sorted[0]);    // key 1
        assertEquals("two", sorted[1]);    // key 2  
        assertEquals("three", sorted[2]);  // key 3
        assertEquals("seven", sorted[3]);  // key 7
        
        // Verify map is still functional after non-destructive sort
        assertEquals("three", map.get(3));
        assertEquals("one", map.get(1));
        assertEquals(4, map.size());
        
        // Should be able to call multiple times
        String[] sorted2 = map.getSortedValuesCopy(new String[0]);
        assertArrayEquals(sorted, sorted2);
    }
    
    @Test
    void shouldSortValuesDestructively() {
        map.put(3, "three");
        map.put(1, "one");
        map.put(7, "seven");
        map.put(2, "two");
        
        // Test destructive sort
        ImprintFieldObjectMap.SortedValuesResult result = map.getSortedValues();
        
        assertEquals(4, result.count);
        assertEquals("one", result.values[0]);    // key 1
        assertEquals("two", result.values[1]);    // key 2
        assertEquals("three", result.values[2]);  // key 3
        assertEquals("seven", result.values[3]);  // key 7
    }
    
    @Test
    void shouldPoisonMapAfterDestructiveSort() {
        map.put(1, "one");
        map.put(2, "two");
        
        // Perform destructive sort
        ImprintFieldObjectMap.SortedValuesResult result = map.getSortedValues();
        assertNotNull(result);
        
        // All operations should throw IllegalStateException after poisoning
        assertThrows(IllegalStateException.class, () -> map.put(3, "three"));
        assertThrows(IllegalStateException.class, () -> map.get(1));
        assertThrows(IllegalStateException.class, () -> map.containsKey(1));
        assertThrows(IllegalStateException.class, () -> map.getSortedValuesCopy(new String[0]));
        
        // Size and isEmpty should still work (they don't check poisoned state)
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
    }
    
    @Test
    void shouldHandleEmptyMapSorting() {
        // Test non-destructive sort on empty map
        String[] sorted = map.getSortedValuesCopy(new String[0]);
        assertEquals(0, sorted.length);
        
        // Test destructive sort on empty map
        ImprintFieldObjectMap.SortedValuesResult result = map.getSortedValues();
        assertEquals(0, result.count);
        
        // Map should be poisoned even after empty destructive sort
        assertThrows(IllegalStateException.class, () -> map.put(1, "one"));
    }
    
    @Test
    void shouldHandleSingleElementSorting() {
        map.put(42, "answer");
        
        // Test non-destructive sort
        String[] sorted = map.getSortedValuesCopy(new String[0]);
        assertEquals(1, sorted.length);
        assertEquals("answer", sorted[0]);
        
        // Test destructive sort on fresh map
        ImprintFieldObjectMap<String> map2 = new ImprintFieldObjectMap<>();
        map2.put(42, "answer");
        
        ImprintFieldObjectMap.SortedValuesResult result = map2.getSortedValues();
        assertEquals(1, result.count);
        assertEquals("answer", result.values[0]);
    }
    
    @Test
    void shouldHandleHashCollisions() {
        // Add many entries to trigger collisions and resizing
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value_" + i);
        }
        
        // Verify all entries are accessible
        for (int i = 0; i < 1000; i++) {
            assertEquals("value_" + i, map.get(i));
            assertTrue(map.containsKey(i));
        }
        
        assertEquals(1000, map.size());
        
        // Test sorting with many entries
        String[] sorted = map.getSortedValuesCopy(new String[0]);
        assertEquals(1000, sorted.length);
        
        // Verify sorting is correct
        for (int i = 0; i < 1000; i++) {
            assertEquals("value_" + i, sorted[i]);
        }
    }
    
    @Test
    void shouldReuseResultArrayForNonDestructiveSort() {
        map.put(1, "one");
        map.put(2, "two");
        
        String[] reusableArray = new String[2];
        String[] result = map.getSortedValuesCopy(reusableArray);
        
        assertSame(reusableArray, result); // Should reuse the same array
        assertEquals("one", result[0]);
        assertEquals("two", result[1]);
        
        // Test with wrong size array - should create new array
        String[] wrongSizeArray = new String[5];
        String[] result2 = map.getSortedValuesCopy(wrongSizeArray);
        
        assertNotSame(wrongSizeArray, result2); // Should create new array
        assertEquals(2, result2.length);
        assertEquals("one", result2[0]);
        assertEquals("two", result2[1]);
    }
    
    @Test
    void shouldHandleMaxShortValue() {
        int maxKey = Short.MAX_VALUE;
        map.put(maxKey, "max_value");
        map.put(0, "zero");
        map.put(maxKey - 1, "almost_max");
        
        assertEquals("max_value", map.get(maxKey));
        assertEquals("zero", map.get(0));
        assertEquals("almost_max", map.get(maxKey - 1));
        
        String[] sorted = map.getSortedValuesCopy(new String[0]);
        assertEquals("zero", sorted[0]);
        assertEquals("almost_max", sorted[1]);
        assertEquals("max_value", sorted[2]);
    }
    
    @Test
    void shouldMaintainSizeCorrectlyWithOverwrites() {
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        
        map.put(1, "first");
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
        
        map.put(1, "overwrite");
        assertEquals(1, map.size()); // Size should not change
        
        map.put(2, "second");
        assertEquals(2, map.size());
        
        map.put(1, "overwrite_again");
        assertEquals(2, map.size()); // Size should not change
    }
    
    @Test
    void shouldStreamKeysWithoutAllocation() {
        map.put(3, "three");
        map.put(1, "one");
        map.put(7, "seven");
        
        // Stream keys without allocation
        java.util.Set<Integer> streamedKeys = map.streamKeys()
            .boxed()
            .collect(java.util.stream.Collectors.toSet());
        
        assertEquals(3, streamedKeys.size());
        assertTrue(streamedKeys.contains(1));
        assertTrue(streamedKeys.contains(3));
        assertTrue(streamedKeys.contains(7));
        
        // Should be able to stream multiple times
        long count = map.streamKeys().count();
        assertEquals(3, count);
        
        // Test operations on stream
        int sum = map.streamKeys().sum();
        assertEquals(11, sum); // 1 + 3 + 7
        
        // Test filtering
        long evenKeys = map.streamKeys().filter(k -> k % 2 == 0).count();
        assertEquals(0, evenKeys);
        
        long oddKeys = map.streamKeys().filter(k -> k % 2 == 1).count();
        assertEquals(3, oddKeys);
    }
    
    @Test
    void shouldThrowOnStreamKeysAfterPoisoning() {
        map.put(1, "one");
        map.put(2, "two");
        
        // Stream should work before poisoning
        assertEquals(2, map.streamKeys().count());
        
        // Poison the map
        map.getSortedValues();
        
        // Stream should throw after poisoning
        assertThrows(IllegalStateException.class, () -> map.streamKeys());
    }
    
    @Test
    void shouldStreamEmptyMapKeys() {
        // Empty map should produce empty stream
        assertEquals(0, map.streamKeys().count());
        
        // Operations on empty stream should work
        assertEquals(0, map.streamKeys().sum());
        assertEquals(java.util.OptionalInt.empty(), map.streamKeys().findFirst());
    }
}