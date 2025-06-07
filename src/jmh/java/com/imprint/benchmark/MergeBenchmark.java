package com.imprint.benchmark;

import com.imprint.core.ImprintRecord;
import com.imprint.core.ImprintRecordBuilder;
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
        var builder = ImprintRecord.builder(first.getHeader().getSchemaId());
        var usedFieldIds = new HashSet<Integer>();
        
        // Copy fields from first record (takes precedence)
        copyFieldsToBuilder(first, builder, usedFieldIds);
        
        // Copy non-conflicting fields from second record
        copyFieldsToBuilder(second, builder, usedFieldIds);
        
        return builder.build();
    }

    private void copyFieldsToBuilder(ImprintRecord record, ImprintRecordBuilder builder, Set<Integer> usedFieldIds) throws Exception {
        for (var entry : record.getDirectory()) {
            int fieldId = entry.getId();
            if (!usedFieldIds.contains(fieldId)) {
                var value = record.getValue(fieldId);
                if (value != null) {
                    builder.field(fieldId, value);
                    usedFieldIds.add(fieldId);
                }
            }
        }
    }

    private ImprintRecord createProductRecord() throws Exception {
        return ImprintRecord.builder(new SchemaId(1, 0x12345678))
            .field(1, Value.fromString("Product"))
            .field(2, Value.fromInt32(12345))
            .field(3, Value.fromString("Laptop"))
            .field(4, Value.fromFloat64(999.99))
            .field(5, Value.fromString("Electronics"))
            .field(6, Value.fromInt32(50)) // stock
            .field(7, Value.fromString("TechCorp"))
            .field(8, Value.fromBoolean(true)) // available
            .build();
    }

    private ImprintRecord createOrderRecord() throws Exception {
        return ImprintRecord.builder(new SchemaId(2, 0x87654321))
            .field(10, Value.fromString("Order"))
            .field(11, Value.fromInt32(67890))
            .field(12, Value.fromInt32(12345)) // product_id (overlaps with product)
            .field(13, Value.fromInt32(2)) // quantity
            .field(14, Value.fromFloat64(1999.98)) // total
            .field(15, Value.fromString("2024-01-15")) // order_date
            .field(16, Value.fromString("shipped")) // status
            .build();
    }

    private ImprintRecord createCustomerRecord() throws Exception {
        return ImprintRecord.builder(new SchemaId(3, 0x11223344))
            .field(20, Value.fromString("Customer"))
            .field(21, Value.fromInt32(555))
            .field(22, Value.fromString("John Doe"))
            .field(23, Value.fromString("john.doe@email.com"))
            .field(24, Value.fromString("123 Main St"))
            .field(25, Value.fromString("premium")) // tier
            .field(26, Value.fromBoolean(true)) // active
            .build();
    }

    private ImprintRecord createRecordWithFields(int startId, int endId, String prefix) throws Exception {
        var builder = ImprintRecord.builder(new SchemaId(1, 0x12345678));
        
        for (int i = startId; i <= endId; i++) {
            builder.field(i, Value.fromString(prefix + "field_" + i));
        }
        
        return builder.build();
    }
}