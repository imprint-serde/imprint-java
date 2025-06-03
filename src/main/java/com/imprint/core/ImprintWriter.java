package com.imprint.core;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.Value;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A writer for constructing ImprintRecords by adding fields sequentially.
 */
public final class ImprintWriter {
    private final SchemaId schemaId;
    private final TreeMap<Integer, Value> fields; // keep fields in sorted order
    
    public ImprintWriter(SchemaId schemaId) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
        this.fields = new TreeMap<>();
    }
    
    /**
     * Adds a field to the record being built.
     */
    public ImprintWriter addField(int id, Value value) {
        Objects.requireNonNull(value, "Value cannot be null");
        this.fields.put(id, value);
        return this;
    }
    
    /**
     * Consumes the writer and builds an ImprintRecord.
     */
    public ImprintRecord build() throws ImprintException {
        var directory = new ArrayList<DirectoryEntry>(fields.size());
        var payloadBuffer = ByteBuffer.allocate(estimatePayloadSize());
        payloadBuffer.order(ByteOrder.LITTLE_ENDIAN);
        
        for (var entry : fields.entrySet()) {
            int fieldId = entry.getKey();
            var value = entry.getValue();
            
            directory.add(new DirectoryEntry(fieldId, value.getTypeCode(), payloadBuffer.position()));
            serializeValue(value, payloadBuffer);
        }
        
        // Create read-only view of the payload without copying
        payloadBuffer.flip(); // limit = position, position = 0
        var payloadView = payloadBuffer.slice().asReadOnlyBuffer();
        
        var header = new Header(new Flags((byte) 0), schemaId, payloadView.remaining());
        return new ImprintRecord(header, directory, payloadView);
    }
    
    private int estimatePayloadSize() throws ImprintException {
        // More accurate estimation to reduce allocations
        int estimatedSize = 0;
        for (var value : fields.values()) {
            estimatedSize += estimateValueSize(value);
        }
        // Add 25% buffer to reduce reallocations
        return Math.max(estimatedSize + (estimatedSize / 4), fields.size() * 16);
    }
    
    /**
     * Estimates the serialized size in bytes for a given value.
     * This method provides size estimates for payload buffer allocation,
     * supporting both array-based and ByteBuffer-based value types.
     * 
     * @param value the value to estimate size for
     * @return estimated size in bytes including type-specific overhead
     */
    private int estimateValueSize(Value value) throws ImprintException {
        // Use TypeHandler for simple types
        switch (value.getTypeCode()) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
            case ARRAY:
            case MAP:
                return value.getTypeCode().getHandler().estimateSize(value);

            case ROW:
                Value.RowValue rowValue = (Value.RowValue) value;
                return rowValue.getValue().estimateSerializedSize();

            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + value.getTypeCode());
        }
    }


    private void serializeValue(Value value, ByteBuffer buffer) throws ImprintException {
        switch (value.getTypeCode()) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
            case ARRAY:
            case MAP:
                value.getTypeCode().getHandler().serialize(value, buffer);
                break;
            //TODO eliminate this switch entirely by implementing a ROW TypeHandler
            case ROW:
                Value.RowValue rowValue = (Value.RowValue) value;
                var serializedRow = rowValue.getValue().serializeToBuffer();
                buffer.put(serializedRow);
                break;
                
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + value.getTypeCode());
        }
    }
}