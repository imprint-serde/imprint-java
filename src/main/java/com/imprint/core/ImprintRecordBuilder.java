package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * A fluent builder for creating ImprintRecord instances with type-safe, 
 * developer-friendly API that eliminates boilerplate Value.fromX() calls.
 * <p>
 * Field IDs can be overwritten - calling field() with the same ID multiple times
 * will replace the previous value. This allows for flexible builder patterns and
 * conditional field updates.
 * <p>
 * Usage:
 * <pre>
 *   var record = ImprintRecord.builder(schemaId)
 *       .field(1, 42)              // int to Int32Value  
 *       .field(2, "hello")         // String to StringValue
 *       .field(1, 100)             // overwrites field 1 with new value
 *       .field(3, 3.14)            // double to Float64Value
 *       .field(4, bytes)           // byte[] to BytesValue
 *       .field(5, true)            // boolean to BoolValue
 *       .nullField(6)              // to NullValue
 *       .build();
 * </pre>
 */
@SuppressWarnings("unused")
public final class ImprintRecordBuilder {
    private final SchemaId schemaId;
    // Custom int→object map optimized for primitive keys
    private final ImprintFieldObjectMap<FieldData> fields = new ImprintFieldObjectMap<>();
    private int estimatedPayloadSize = 0;

    static final class FieldData {
        final short id;
        final Value value;
        
        FieldData(short id, Value value) {
            this.id = id;
            this.value = value;
        }
    }
    

    ImprintRecordBuilder(SchemaId schemaId) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
    }

    // Primitive types with automatic Value wrapping
    public ImprintRecordBuilder field(int id, boolean value) {
        return addField(id, Value.fromBoolean(value));
    }

    public ImprintRecordBuilder field(int id, int value) {
        return addField(id, Value.fromInt32(value));
    }

    public ImprintRecordBuilder field(int id, long value) {
        return addField(id, Value.fromInt64(value));
    }

    public ImprintRecordBuilder field(int id, float value) {
        return addField(id, Value.fromFloat32(value));
    }

    public ImprintRecordBuilder field(int id, double value) {
        return addField(id, Value.fromFloat64(value));
    }

    public ImprintRecordBuilder field(int id, String value) {
        return addField(id, Value.fromString(value));
    }

    public ImprintRecordBuilder field(int id, byte[] value) {
        return addField(id, Value.fromBytes(value));
    }

    // Collections with automatic conversion
    public ImprintRecordBuilder field(int id, List<?> values) {
        var convertedValues = new ArrayList<Value>(values.size());
        for (var item : values) {
            convertedValues.add(convertToValue(item));
        }
        return addField(id, Value.fromArray(convertedValues));
    }

    public ImprintRecordBuilder field(int id, Map<?, ?> map) {
        var convertedMap = new HashMap<MapKey, Value>(map.size());
        for (var entry : map.entrySet()) {
            var key = convertToMapKey(entry.getKey());
            var value = convertToValue(entry.getValue());
            convertedMap.put(key, value);
        }
        return addField(id, Value.fromMap(convertedMap));
    }

    // Nested records
    public ImprintRecordBuilder field(int id, ImprintRecord nestedRecord) {
        return addField(id, Value.fromRow(nestedRecord));
    }

    // Explicit null field
    public ImprintRecordBuilder nullField(int id) {
        return addField(id, Value.nullValue());
    }

    // Direct Value API (escape hatch for advanced usage)
    public ImprintRecordBuilder field(int id, Value value) {
        return addField(id, value);
    }

    // Conditional field addition
    public ImprintRecordBuilder fieldIf(boolean condition, int id, Object value) {
        if (condition) {
            return field(id, convertToValue(value));
        }
        return this;
    }

    public ImprintRecordBuilder fieldIfNotNull(int id, Object value) {
        return fieldIf(value != null, id, value);
    }

    // Bulk operations
    public ImprintRecordBuilder fields(Map<Integer, ?> fieldsMap) {
        for (var entry : fieldsMap.entrySet()) {
            field(entry.getKey(), convertToValue(entry.getValue()));
        }
        return this;
    }

    // Builder utilities
    public boolean hasField(int id) {
        return fields.containsKey(id);
    }

    public int fieldCount() {
        return fields.size();
    }

    public Set<Integer> fieldIds() {
        var ids = new HashSet<Integer>(fields.size());
        var keys = fields.getKeys();
        for (var key : keys) {
            ids.add(key);
        }
        return ids;
    }

    // Build the final record
    public ImprintRecord build() throws ImprintException {
        // Build to bytes and then create ImprintRecord from bytes for consistency
        var serializedBytes = buildToBuffer();
        return ImprintRecord.fromBytes(serializedBytes);
    }

    /**
     * Builds the record and serializes it directly to a ByteBuffer.
     *
     * @return A read-only ByteBuffer containing the fully serialized record.
     * @throws ImprintException if serialization fails.
     */
    public ByteBuffer buildToBuffer() throws ImprintException {
        // 1. Sort fields by ID for directory ordering (zero allocation)
        var sortedFieldsResult = getSortedFieldsResult();
        var sortedFields = sortedFieldsResult.values;
        var fieldCount = sortedFieldsResult.count;
        
        // 2. Serialize payload and calculate offsets
        var payloadBuffer = ByteBuffer.allocate(estimatePayloadSize());
        payloadBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int[] offsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            var fieldData = (FieldData) sortedFields[i];
            offsets[i] = payloadBuffer.position();
            serializeValue(fieldData.value, payloadBuffer);
        }
        payloadBuffer.flip();
        var payloadView = payloadBuffer.slice().asReadOnlyBuffer();

        // 3. Create directory buffer and serialize to final buffer
        return serializeToBuffer(schemaId, sortedFields, offsets, fieldCount, payloadView);
    }

    /**
     * Adds or overwrites a field in the record being built.
     * If a field with the given ID already exists, it will be replaced.
     *
     * @param id the field ID
     * @param value the field value (cannot be null - use nullField() for explicit nulls)
     * @return this builder for method chaining
     */
    private ImprintRecordBuilder addField(int id, Value value) {
        Objects.requireNonNull(value, "Value cannot be null - use nullField() for explicit null values");
        var newEntry = new FieldData((short) id, value);

        // Check if replacing an existing field - O(1) lookup without boxing!
        var oldEntry = fields.get(id);
        if (oldEntry != null) {
            estimatedPayloadSize -= estimateValueSize(oldEntry.value);
        }

        // Add or replace field - O(1) operation without boxing!
        fields.put(id, newEntry);
        estimatedPayloadSize += estimateValueSize(newEntry.value);
        return this;
    }

    private Value convertToValue(Object obj) {
        if (obj == null) {
            return Value.nullValue();
        }

        if (obj instanceof Value) {
            return (Value) obj;
        }

        // Auto-boxing conversion
        if (obj instanceof Boolean) {
            return Value.fromBoolean((Boolean) obj);
        }
        if (obj instanceof Integer) {
            return Value.fromInt32((Integer) obj);
        }
        if (obj instanceof Long) {
            return Value.fromInt64((Long) obj);
        }
        if (obj instanceof Float) {
            return Value.fromFloat32((Float) obj);
        }
        if (obj instanceof Double) {
            return Value.fromFloat64((Double) obj);
        }
        if (obj instanceof String) {
            return Value.fromString((String) obj);
        }
        if (obj instanceof byte[]) {
            return Value.fromBytes((byte[]) obj);
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) obj;
            var convertedValues = new ArrayList<Value>(list.size());
            for (var item : list) {
                convertedValues.add(convertToValue(item));
            }
            return Value.fromArray(convertedValues);
        }
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) obj;
            var convertedMap = new HashMap<MapKey, Value>(map.size());
            for (var entry : map.entrySet()) {
                var key = convertToMapKey(entry.getKey());
                var value = convertToValue(entry.getValue());
                convertedMap.put(key, value);
            }
            return Value.fromMap(convertedMap);
        }
        if (obj instanceof ImprintRecord) {
            return Value.fromRow((ImprintRecord) obj);
        }

        throw new IllegalArgumentException("Unsupported type for auto-conversion: " + obj.getClass().getName());
    }

    private MapKey convertToMapKey(Object obj) {
        if (obj instanceof Integer) {
            return MapKey.fromInt32((Integer) obj);
        }
        if (obj instanceof Long) {
            return MapKey.fromInt64((Long) obj);
        }
        if (obj instanceof String) {
            return MapKey.fromString((String) obj);
        }
        if (obj instanceof byte[]) {
            return MapKey.fromBytes((byte[]) obj);
        }

        throw new IllegalArgumentException("Unsupported map key type: " + obj.getClass().getName());
    }

    private int estimatePayloadSize() {
        // Add 25% buffer to reduce reallocations and handle VarInt encoding fluctuations.
        return Math.max(estimatedPayloadSize + (estimatedPayloadSize / 4), fields.size() * 16);
    }

    /**
     * Estimates the serialized size in bytes for a given value.
     *
     * @param value the value to estimate size for
     * @return estimated size in bytes including type-specific overhead
     */
    @SneakyThrows
    private int estimateValueSize(Value value) {
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

    /**
     * Get fields sorted by ID from the map.
     * Returns internal map array reference + count to avoid any copying but sacrifices the map structure in the process.
     */
    private ImprintFieldObjectMap.SortedValuesResult getSortedFieldsResult() {
        return fields.getSortedValues();
    }

    /**
     * Serialize components into a single ByteBuffer.
     */
    private static ByteBuffer serializeToBuffer(SchemaId schemaId, Object[] sortedFields, int[] offsets, int fieldCount, ByteBuffer payload) {
        var header = new Header(new Flags((byte) 0), schemaId, payload.remaining());
        var directoryBuffer = ImprintRecord.createDirectoryBufferFromSorted(sortedFields, offsets, fieldCount);

        int finalSize = Constants.HEADER_BYTES + directoryBuffer.remaining() + payload.remaining();
        var finalBuffer = ByteBuffer.allocate(finalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write header
        finalBuffer.put(Constants.MAGIC);
        finalBuffer.put(Constants.VERSION);
        finalBuffer.put(header.getFlags().getValue());
        finalBuffer.putInt(header.getSchemaId().getFieldSpaceId());
        finalBuffer.putInt(header.getSchemaId().getSchemaHash());
        finalBuffer.putInt(header.getPayloadSize());
        finalBuffer.put(directoryBuffer);
        finalBuffer.put(payload);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }
}