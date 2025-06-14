package com.imprint.benchmark;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataGenerator {

    /**
     * A standard record used for serialization benchmarks.
     * Contains a mix of common data types.
     */
    public static class TestRecord {
        public String id;
        public long timestamp;
        public int flags;
        public boolean active;
        public double value;
        public byte[] data;
        public List<Integer> tags;
        public Map<String, String> metadata;
    }

    /**
     * A smaller record representing a projection of the full TestRecord.
     */
    public static class ProjectedRecord {
        public String id;
        public long timestamp;
        public List<Integer> tags;
    }

    public static TestRecord createTestRecord() {
        var record = new TestRecord();
        record.id = "ID" + System.nanoTime();
        record.timestamp = System.currentTimeMillis();
        record.flags = 0xDEADBEEF;
        record.active = true;
        record.value = Math.PI;
        record.data = createBytes(128);
        record.tags = createIntList(20);
        record.metadata = createStringMap(10);
        return record;
    }

    public static byte[] createBytes(int size) {
        byte[] bytes = new byte[size];
        new Random(0).nextBytes(bytes);
        return bytes;
    }

    public static List<Integer> createIntList(int size) {
        return IntStream.range(0, size).boxed().collect(Collectors.toList());
    }

    public static Map<String, String> createStringMap(int size) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put("key" + i, "value" + i);
        }
        return map;
    }
} 