package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintWriter;
import com.imprint.core.SchemaId;
import com.imprint.types.Value;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for ImprintRecord merge operations.
 * NOTE: These benchmarks simulate merge operations until the actual merge API is implemented.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MergeBenchmark {

    private ImprintRecord productRecord;
    private ImprintRecord orderRecord;
    private ImprintRecord customerRecord;

    @Setup
    public void setup() throws Exception {
        productRecord = createProductRecord();
        orderRecord = createOrderRecord();
        customerRecord = createCustomerRecord();
    }

    // ===== SIMULATED MERGE BENCHMARKS =====
    // These will be replaced with actual merge API when implemented

    @Benchmark
    public void mergeProductAndOrder(Blackhole bh) throws Exception {
        // Simulate merge by creating a new record with fields from both
        ImprintRecord result = simulateMerge(productRecord, orderRecord);
        bh.consume(result);
    }

    @Benchmark
    public void mergeProductAndCustomer(Blackhole bh) throws Exception {
        ImprintRecord result = simulateMerge(productRecord, customerRecord);
        bh.consume(result);
    }

    @Benchmark
    public void mergeOrderAndCustomer(Blackhole bh) throws Exception {
        ImprintRecord result = simulateMerge(orderRecord, customerRecord);
        bh.consume(result);
    }

    @Benchmark
    public void mergeThreeRecords(Blackhole bh) throws Exception {
        // Test merging multiple records
        var temp = simulateMerge(productRecord, orderRecord);
        ImprintRecord result = simulateMerge(temp, customerRecord);
        bh.consume(result);
    }

    // ===== MERGE CONFLICT HANDLING =====

    @Benchmark
    public void mergeWithConflicts(Blackhole bh) throws Exception {
        // Create records with overlapping field IDs to test conflict resolution
        var record1 = createRecordWithFields(1, 50, "record1_");
        var record2 = createRecordWithFields(25, 75, "record2_");
        
        ImprintRecord result = simulateMerge(record1, record2);
        bh.consume(result);
    }

    // ===== HELPER METHODS =====

    /**
     * Simulates merge operation by manually copying fields.
     * This should be replaced with actual merge API when available.
     */
    private ImprintRecord simulateMerge(ImprintRecord first, ImprintRecord second) throws Exception {
        var writer = new ImprintWriter(first.getHeader().getSchemaId());
        var usedFieldIds = new HashSet<Integer>();
        
        // Copy fields from first record (takes precedence)
        copyFieldsToWriter(first, writer, usedFieldIds);
        
        // Copy non-conflicting fields from second record
        copyFieldsToWriter(second, writer, usedFieldIds);
        
        return writer.build();
    }

    private void copyFieldsToWriter(ImprintRecord record, ImprintWriter writer, Set<Integer> usedFieldIds) throws Exception {
        for (var entry : record.getDirectory()) {
            int fieldId = entry.getId();
            if (!usedFieldIds.contains(fieldId)) {
                var value = record.getValue(fieldId);
                if (value != null) {
                    writer.addField(fieldId, value);
                    usedFieldIds.add(fieldId);
                }
            }
        }
    }

    private ImprintRecord createProductRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        writer.addField(1, Value.fromString("Product"));
        writer.addField(2, Value.fromInt32(12345));
        writer.addField(3, Value.fromString("Laptop"));
        writer.addField(4, Value.fromFloat64(999.99));
        writer.addField(5, Value.fromString("Electronics"));
        writer.addField(6, Value.fromInt32(50)); // stock
        writer.addField(7, Value.fromString("TechCorp"));
        writer.addField(8, Value.fromBoolean(true)); // available
        
        return writer.build();
    }

    private ImprintRecord createOrderRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(2, 0x87654321));
        
        writer.addField(10, Value.fromString("Order"));
        writer.addField(11, Value.fromInt32(67890));
        writer.addField(12, Value.fromInt32(12345)); // product_id (overlaps with product)
        writer.addField(13, Value.fromInt32(2)); // quantity
        writer.addField(14, Value.fromFloat64(1999.98)); // total
        writer.addField(15, Value.fromString("2024-01-15")); // order_date
        writer.addField(16, Value.fromString("shipped")); // status
        
        return writer.build();
    }

    private ImprintRecord createCustomerRecord() throws Exception {
        var writer = new ImprintWriter(new SchemaId(3, 0x11223344));
        
        writer.addField(20, Value.fromString("Customer"));
        writer.addField(21, Value.fromInt32(555));
        writer.addField(22, Value.fromString("John Doe"));
        writer.addField(23, Value.fromString("john.doe@email.com"));
        writer.addField(24, Value.fromString("123 Main St"));
        writer.addField(25, Value.fromString("premium")); // tier
        writer.addField(26, Value.fromBoolean(true)); // active
        
        return writer.build();
    }

    private ImprintRecord createRecordWithFields(int startId, int endId, String prefix) throws Exception {
        var writer = new ImprintWriter(new SchemaId(1, 0x12345678));
        
        for (int i = startId; i <= endId; i++) {
            writer.addField(i, Value.fromString(prefix + "field_" + i));
        }
        
        return writer.build();
    }
}