package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.ImprintSerializers;
import com.imprint.types.MapKey;
import com.imprint.types.TypeCode;
import com.imprint.util.VarInt;
import lombok.SneakyThrows;

import java.nio.BufferOverflowException;
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
    private final ImprintFieldObjectMap<FieldValue> fields;
    private int estimatedPayloadSize = 0;

    // Direct primitive storage to avoid Value object creation
    @lombok.Value
    static class FieldValue {
        byte typeCode;
        Object value;
        
        // Fast factory methods for primitives
        static FieldValue ofInt32(int value) { return new FieldValue(TypeCode.INT32.getCode(), value); }
        static FieldValue ofInt64(long value) { return new FieldValue(TypeCode.INT64.getCode(), value); }
        static FieldValue ofFloat32(float value) { return new FieldValue(TypeCode.FLOAT32.getCode(), value); }
        static FieldValue ofFloat64(double value) { return new FieldValue(TypeCode.FLOAT64.getCode(), value); }
        static FieldValue ofBool(boolean value) { return new FieldValue(TypeCode.BOOL.getCode(), value); }
        static FieldValue ofString(String value) { return new FieldValue(TypeCode.STRING.getCode(), value); }
        static FieldValue ofBytes(byte[] value) { return new FieldValue(TypeCode.BYTES.getCode(), value); }
        static FieldValue ofArray(List<?> value) { return new FieldValue(TypeCode.ARRAY.getCode(), value); }
        static FieldValue ofMap(Map<?, ?> value) { return new FieldValue(TypeCode.MAP.getCode(), value); }
        static FieldValue ofNull() { return new FieldValue(TypeCode.NULL.getCode(), null); }
    }

    ImprintRecordBuilder(SchemaId schemaId) {
        this(schemaId, 16); // Default capacity for typical usage (8-16 fields)
    }
    
    ImprintRecordBuilder(SchemaId schemaId, int expectedFieldCount) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
        this.fields = new ImprintFieldObjectMap<>(expectedFieldCount);
    }


    public ImprintRecordBuilder field(int id, boolean value) {
        return addField(id, FieldValue.ofBool(value));
    }

    public ImprintRecordBuilder field(int id, int value) {
        return addField(id, FieldValue.ofInt32(value));
    }

    public ImprintRecordBuilder field(int id, long value) {
        return addField(id, FieldValue.ofInt64(value));
    }

    public ImprintRecordBuilder field(int id, float value) {
        return addField(id, FieldValue.ofFloat32(value));
    }

    public ImprintRecordBuilder field(int id, double value) {
        return addField(id, FieldValue.ofFloat64(value));
    }

    public ImprintRecordBuilder field(int id, String value) {
        return addField(id, FieldValue.ofString(value));
    }

    public ImprintRecordBuilder field(int id, byte[] value) {
        return addField(id, FieldValue.ofBytes(value));
    }

    // Collections - store as raw collections, convert during serialization
    public ImprintRecordBuilder field(int id, List<?> values) {
        return addField(id, FieldValue.ofArray(values));
    }

    public ImprintRecordBuilder field(int id, Map<?, ?> map) {
        return addField(id, FieldValue.ofMap(map));
    }

    // Nested records
    public ImprintRecordBuilder field(int id, ImprintRecord nestedRecord) {
        return addField(id, new FieldValue(TypeCode.ROW.getCode(), nestedRecord));
    }

    // Explicit null field
    public ImprintRecordBuilder nullField(int id) {
        return addField(id, FieldValue.ofNull());
    }

    // Conditional field addition
    public ImprintRecordBuilder fieldIf(boolean condition, int id, Object value) {
        if (condition) {
            return addField(id, convertToFieldValue(value));
        }
        return this;
    }

    public ImprintRecordBuilder fieldIfNotNull(int id, Object value) {
        return fieldIf(value != null, id, value);
    }

    // Bulk operations
    public ImprintRecordBuilder fields(Map<Integer, ?> fieldsMap) {
        for (var entry : fieldsMap.entrySet()) {
            addField(entry.getKey(), convertToFieldValue(entry.getValue()));
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
        // 1. Calculate conservative size BEFORE sorting (which invalidates the map)
        int conservativeSize = calculateConservativePayloadSize();

        // 2. Sort fields by ID for directory ordering (zero allocation)
        var sortedFieldsResult = getSortedFieldsResult();
        var sortedValues = sortedFieldsResult.values;
        var sortedKeys = sortedFieldsResult.keys;
        var fieldCount = sortedFieldsResult.count;

        // 3. Serialize payload and calculate offsets with overflow handling
        PayloadSerializationResult result = null;
        int bufferSizeMultiplier = 1;

        while (result == null && bufferSizeMultiplier <= 64) {
            try {
                result = serializePayload(sortedValues, fieldCount, conservativeSize, bufferSizeMultiplier);
            } catch (BufferOverflowException e) {
                bufferSizeMultiplier *= 2; // Try 2x, 4x, 8x, 16x, 32x, 64x
            }
        }

        if (result == null) {
            throw new ImprintException(ErrorType.SERIALIZATION_ERROR,
                    "Failed to serialize payload even with 64x buffer size");
        }

        return serializeToBuffer(schemaId, sortedKeys, sortedValues, result.offsets, fieldCount, result.payload);
    }

    /**
     * Adds or overwrites a field in the record being built.
     * If a field with the given ID already exists, it will be replaced.
     *
     * @param id the field ID
     * @param fieldValue the field value with type code
     * @return this builder for method chaining
     */
    @SneakyThrows
    private ImprintRecordBuilder addField(int id, FieldValue fieldValue) {
        Objects.requireNonNull(fieldValue, "FieldValue cannot be null");
        int newSize = estimateFieldSize(fieldValue);
        var oldEntry = fields.putAndReturnOld(id, fieldValue);

        if (oldEntry != null) {
            int oldSize = estimateFieldSize(oldEntry);
            estimatedPayloadSize += newSize - oldSize;
        } else {
            estimatedPayloadSize += newSize;
        }

        return this;
    }

    private FieldValue convertToFieldValue(Object obj) {
        if (obj == null) {
            return FieldValue.ofNull();
        }
        if (obj instanceof Boolean) {
            return FieldValue.ofBool((Boolean) obj);
        }
        if (obj instanceof Integer) {
            return FieldValue.ofInt32((Integer) obj);
        }
        if (obj instanceof Long) {
            return FieldValue.ofInt64((Long) obj);
        }
        if (obj instanceof Float) {
            return FieldValue.ofFloat32((Float) obj);
        }
        if (obj instanceof Double) {
            return FieldValue.ofFloat64((Double) obj);
        }
        if (obj instanceof String) {
            return FieldValue.ofString((String) obj);
        }
        if (obj instanceof byte[]) {
            return FieldValue.ofBytes((byte[]) obj);
        }
        if (obj instanceof List) {
            return FieldValue.ofArray((List<?>) obj);
        }
        if (obj instanceof Map) {
            return FieldValue.ofMap((Map<?, ?>) obj);
        }
        if (obj instanceof ImprintRecord) {
            return new FieldValue(TypeCode.ROW.getCode(), obj);
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

    private int estimateFieldSize(FieldValue fieldValue) {
        TypeCode typeCode;
        try {
            typeCode = TypeCode.fromByte(fieldValue.typeCode);
        } catch (ImprintException e) {
            throw new RuntimeException("Invalid type code in FieldValue: " + fieldValue.typeCode, e);
        }
        return ImprintSerializers.estimateSize(typeCode, fieldValue.value);
    }

    private int calculateConservativePayloadSize() {
        // Add 25% buffer for safety margin
        return Math.max(estimatedPayloadSize + (estimatedPayloadSize / 4), 4096);
    }

    private static class PayloadSerializationResult {
        final int[] offsets;
        final ByteBuffer payload;

        PayloadSerializationResult(int[] offsets, ByteBuffer payload) {
            this.offsets = offsets;
            this.payload = payload;
        }
    }

    private PayloadSerializationResult serializePayload(Object[] sortedFields, int fieldCount, int conservativeSize, int sizeMultiplier) throws ImprintException {
        var payloadBuffer = ByteBuffer.allocate(conservativeSize * sizeMultiplier);
        payloadBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return doSerializePayload(sortedFields, fieldCount, payloadBuffer);
    }

    private PayloadSerializationResult doSerializePayload(Object[] sortedFields, int fieldCount, ByteBuffer payloadBuffer) throws ImprintException {
        int[] offsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            var fieldValue = (FieldValue) sortedFields[i];
            offsets[i] = payloadBuffer.position();
            serializeFieldValue(fieldValue, payloadBuffer);
        }
        payloadBuffer.flip();
        var payloadView = payloadBuffer.slice().asReadOnlyBuffer();
        return new PayloadSerializationResult(offsets, payloadView);
    }

    private void serializeFieldValue(FieldValue fieldValue, ByteBuffer buffer) throws ImprintException {
        var typeCode = TypeCode.fromByte(fieldValue.typeCode);
        var value = fieldValue.value;
        switch (typeCode) {
            case NULL:
                ImprintSerializers.serializeNull(buffer);
                break;
            case BOOL:
                ImprintSerializers.serializeBool((Boolean) value, buffer);
                break;
            case INT32:
                ImprintSerializers.serializeInt32((Integer) value, buffer);
                break;
            case INT64:
                ImprintSerializers.serializeInt64((Long) value, buffer);
                break;
            case FLOAT32:
                ImprintSerializers.serializeFloat32((Float) value, buffer);
                break;
            case FLOAT64:
                ImprintSerializers.serializeFloat64((Double) value, buffer);
                break;
            case STRING:
                ImprintSerializers.serializeString((String) value, buffer);
                break;
            case BYTES:
                ImprintSerializers.serializeBytes((byte[]) value, buffer);
                break;
            case ARRAY:
                serializeArray((List<?>) value, buffer);
                break;
            case MAP:
                serializeMap((Map<?, ?>) value, buffer);
                break;
            case ROW:
                // Nested records
                var nestedRecord = (ImprintRecord) value;
                var serializedRow = nestedRecord.serializeToBuffer();
                buffer.put(serializedRow);
                break;
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + typeCode);
        }
    }

    //TODO arrays and maps need to be handled better
    private void serializeArray(List<?> list, ByteBuffer buffer) throws ImprintException {
        ImprintSerializers.serializeArray(list, buffer,
            this::getTypeCodeForObject, 
            this::serializeObjectDirect);
    }
    
    private void serializeMap(Map<?, ?> map, ByteBuffer buffer) throws ImprintException {
        ImprintSerializers.serializeMap(map, buffer,
            this::convertToMapKey,
            this::getTypeCodeForObject,
            this::serializeObjectDirect);
    }

    private TypeCode getTypeCodeForObject(Object obj) {
        var fieldValue = convertToFieldValue(obj);
        try {
            return TypeCode.fromByte(fieldValue.typeCode);
        } catch (ImprintException e) {
            throw new RuntimeException("Invalid type code", e);
        }
    }
    
    private void serializeObjectDirect(Object obj, ByteBuffer buffer) {
        try {
            var fieldValue = convertToFieldValue(obj);
            serializeFieldValue(fieldValue, buffer);
        } catch (ImprintException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    /**
     * Get fields sorted by ID from the map.
     * Returns internal map array reference + count to avoid any copying but sacrifices the map structure in the process.
     */
    private ImprintFieldObjectMap.SortedFieldsResult getSortedFieldsResult() {
        return fields.getSortedFields();
    }

    /**
     * Serialize components into a single ByteBuffer.
     */
    private static ByteBuffer serializeToBuffer(SchemaId schemaId, short[] sortedKeys, Object[] sortedValues, int[] offsets, int fieldCount, ByteBuffer payload) {
        var header = new Header(new Flags((byte) 0), schemaId, payload.remaining());
        int directorySize = ImprintRecord.calculateDirectorySize(fieldCount);

        int finalSize = Constants.HEADER_BYTES + directorySize + payload.remaining();
        var finalBuffer = ByteBuffer.allocate(finalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write header
        finalBuffer.put(Constants.MAGIC);
        finalBuffer.put(Constants.VERSION);
        finalBuffer.put(header.getFlags().getValue());
        finalBuffer.putInt(header.getSchemaId().getFieldSpaceId());
        finalBuffer.putInt(header.getSchemaId().getSchemaHash());
        finalBuffer.putInt(header.getPayloadSize());

        // Write directory with FieldValue type codes
        writeDirectoryToBuffer(sortedKeys, sortedValues, offsets, fieldCount, finalBuffer);

        // Write payload
        finalBuffer.put(payload);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }
    
    /**
     * Write directory entries directly to buffer for FieldValue objects.
     */
    private static void writeDirectoryToBuffer(short[] sortedKeys, Object[] sortedValues, int[] offsets, int fieldCount, ByteBuffer buffer) {
        // Write field count at the beginning of directory
        // Optimize VarInt encoding for common case (< 128 fields = single byte)
        if (fieldCount < 128) {
            buffer.put((byte) fieldCount);
        } else {
            VarInt.encode(fieldCount, buffer);
        }

        // Early return for empty directory
        if (fieldCount == 0)
            return;


        //hopefully JIT vectorizes this
        for (int i = 0; i < fieldCount; i++) {
            var fieldValue = (FieldValue) sortedValues[i];
            int pos = buffer.position();
            buffer.putShort(pos, sortedKeys[i]);                  // bytes 0-1: field ID
            buffer.put(pos + 2, fieldValue.typeCode);      // byte 2: type code
            buffer.putInt(pos + 3, offsets[i]);            // bytes 3-6: offset
            // Advance buffer position by 7 bytes
            buffer.position(pos + 7);
        }
    }

}
