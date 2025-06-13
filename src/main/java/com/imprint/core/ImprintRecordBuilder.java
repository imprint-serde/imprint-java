package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.TypeCode;
import com.imprint.types.Value;
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
    private final ImprintFieldObjectMap<ValueWithType> fields = new ImprintFieldObjectMap<>();
    private int estimatedPayloadSize = 0;

    // Minimal wrapper to avoid getTypeCode() virtual dispatch
    static final class ValueWithType {
        final Value value;
        final byte typeCode;

        ValueWithType(Value value, byte typeCode) {
            this.value = value;
            this.typeCode = typeCode;
        }
    }

    ImprintRecordBuilder(SchemaId schemaId) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
    }

    // Primitive types with automatic Value wrapping
    public ImprintRecordBuilder field(int id, boolean value) {
        return addField(id, Value.fromBoolean(value), TypeCode.BOOL);
    }

    public ImprintRecordBuilder field(int id, int value) {
        return addField(id, Value.fromInt32(value), TypeCode.INT32);
    }

    public ImprintRecordBuilder field(int id, long value) {
        return addField(id, Value.fromInt64(value), TypeCode.INT64);
    }

    public ImprintRecordBuilder field(int id, float value) {
        return addField(id, Value.fromFloat32(value), TypeCode.FLOAT32);
    }

    public ImprintRecordBuilder field(int id, double value) {
        return addField(id, Value.fromFloat64(value), TypeCode.FLOAT64);
    }

    public ImprintRecordBuilder field(int id, String value) {
        return addField(id, Value.fromString(value), TypeCode.STRING);
    }

    public ImprintRecordBuilder field(int id, byte[] value) {
        return addField(id, Value.fromBytes(value), TypeCode.BYTES);
    }

    // Collections with automatic conversion
    public ImprintRecordBuilder field(int id, List<?> values) {
        var convertedValues = new ArrayList<Value>(values.size());
        for (var item : values) {
            convertedValues.add(convertToValue(item));
        }
        return addField(id, Value.fromArray(convertedValues), TypeCode.ARRAY);
    }

    public ImprintRecordBuilder field(int id, Map<?, ?> map) {
        var convertedMap = new HashMap<MapKey, Value>(map.size());
        for (var entry : map.entrySet()) {
            var key = convertToMapKey(entry.getKey());
            var value = convertToValue(entry.getValue());
            convertedMap.put(key, value);
        }
        return addField(id, Value.fromMap(convertedMap), TypeCode.MAP);
    }

    // Nested records
    public ImprintRecordBuilder field(int id, ImprintRecord nestedRecord) {
        return addField(id, Value.fromRow(nestedRecord), TypeCode.ROW);
    }

    // Explicit null field
    public ImprintRecordBuilder nullField(int id) {
        return addField(id, Value.nullValue(), TypeCode.NULL);
    }

    // Direct Value API (escape hatch for advanced usage)
    public ImprintRecordBuilder field(int id, Value value) {
        return addField(id, value, value.getTypeCode()); // Only virtual call when type is unknown
    }

    // Conditional field addition
    public ImprintRecordBuilder fieldIf(boolean condition, int id, Object value) {
        if (condition) {
            var convertedValue = convertToValue(value);
            return addField(id, convertedValue, convertedValue.getTypeCode());
        }
        return this;
    }

    public ImprintRecordBuilder fieldIfNotNull(int id, Object value) {
        return fieldIf(value != null, id, value);
    }

    // Bulk operations
    public ImprintRecordBuilder fields(Map<Integer, ?> fieldsMap) {
        for (var entry : fieldsMap.entrySet()) {
            var convertedValue = convertToValue(entry.getValue());
            addField(entry.getKey(), convertedValue, convertedValue.getTypeCode());
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
     * @param value the field value (cannot be null - use nullField() for explicit nulls)
     * @param typeCode the known type code (avoids virtual call)
     * @return this builder for method chaining
     */
    @SneakyThrows
    private ImprintRecordBuilder addField(int id, Value value, TypeCode typeCode) {
        Objects.requireNonNull(value, "Value cannot be null - use nullField() for explicit null values");

        // Calculate size for tracking (but don't store it)
        int newSize = fastEstimateFieldSize(value, typeCode);

        // Create wrapper to avoid virtual dispatch later
        var newEntry = new ValueWithType(value, typeCode.getCode());

        // Efficient put with old value return - single hash operation
        var oldEntry = fields.putAndReturnOld(id, newEntry);

        if (oldEntry != null) {
            // Field replacement - subtract old size, add new size
            int oldSize = fastEstimateFieldSize(oldEntry.value, TypeCode.fromByte(oldEntry.typeCode));
            estimatedPayloadSize += newSize - oldSize;
        } else {
            // New field - just add new size
            estimatedPayloadSize += newSize;
        }

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

    /**
     * Fast conservative field size estimation optimized for performance.
     * Uses minimal operations while handling both normal and large data correctly.
     */
    private int fastEstimateFieldSize(Value value, TypeCode typeCode) {
        switch (typeCode) {
            case NULL: return 0;
            case BOOL: return 1;
            case INT32:
            case FLOAT32:
                return 4;
            case INT64:
            case FLOAT64:
                return 8;
            case STRING:
                // Smart estimation: check if it's a large string with minimal overhead
                if (value instanceof Value.StringValue) {
                    int len = ((Value.StringValue) value).getValue().length();
                    return len > 1000 ? 5 + len * 3 : 256; // UTF-8 worst case for large strings
                } else {
                    int remaining = ((Value.StringBufferValue) value).getBuffer().remaining();
                    return remaining > 1000 ? 5 + remaining : 256;
                }
            case BYTES:
                // Smart estimation: check if it's large bytes with minimal overhead
                if (value instanceof Value.BytesValue) {
                    int len = ((Value.BytesValue) value).getValue().length;
                    return len > 1000 ? 5 + len : 256;
                } else {
                    int remaining = ((Value.BytesBufferValue) value).getBuffer().remaining();
                    return remaining > 1000 ? 5 + remaining : 256;
                }
            case ARRAY:
                return 512; // Conservative: most arrays are < 512 bytes
            case MAP:
                return 512; // Conservative: most maps are < 512 bytes
            case ROW:
                return 1024; // Conservative: most nested records are < 1KB

            default:
                return 64; // Fallback
        }
    }

    /**
     * Get current estimated payload size with 25% buffer.
     */
    private int calculateConservativePayloadSize() {
        // Add 25% buffer for safety margin
        return Math.max(estimatedPayloadSize + (estimatedPayloadSize / 4), 4096);
    }


    /**
     * Result of payload serialization containing offsets and final payload buffer.
     */
    private static class PayloadSerializationResult {
        final int[] offsets;
        final ByteBuffer payload;

        PayloadSerializationResult(int[] offsets, ByteBuffer payload) {
            this.offsets = offsets;
            this.payload = payload;
        }
    }

    /**
     * Serialize payload with conservative buffer size multiplier.
     */
    private PayloadSerializationResult serializePayload(Object[] sortedFields, int fieldCount, int conservativeSize, int sizeMultiplier) throws ImprintException {
        var payloadBuffer = ByteBuffer.allocate(conservativeSize * sizeMultiplier);
        payloadBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return doSerializePayload(sortedFields, fieldCount, payloadBuffer);
    }

    /**
     * Core payload serialization logic.
     */
    private PayloadSerializationResult doSerializePayload(Object[] sortedFields, int fieldCount, ByteBuffer payloadBuffer) throws ImprintException {
        int[] offsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            var entry = (ValueWithType) sortedFields[i];
            offsets[i] = payloadBuffer.position();
            serializeValue(entry.value, payloadBuffer);
        }
        payloadBuffer.flip();
        var payloadView = payloadBuffer.slice().asReadOnlyBuffer();
        return new PayloadSerializationResult(offsets, payloadView);
    }

    private void serializeValue(Value value, ByteBuffer buffer) throws ImprintException {
        var typeCode = value.getTypeCode();
        switch (typeCode) {
            case NULL:
                // NULL values have no payload
                break;
            case BOOL:
                Serializers.serializeBool((Value.BoolValue) value, buffer);
                break;
            case INT32:
                Serializers.serializeInt32((Value.Int32Value) value, buffer);
                break;
            case INT64:
                Serializers.serializeInt64((Value.Int64Value) value, buffer);
                break;
            case FLOAT32:
                Serializers.serializeFloat32((Value.Float32Value) value, buffer);
                break;
            case FLOAT64:
                Serializers.serializeFloat64((Value.Float64Value) value, buffer);
                break;
            case STRING:
                Serializers.serializeString(value, buffer);
                break;
            case BYTES:
                Serializers.serializeBytes(value, buffer);
                break;
            case ARRAY:
                Serializers.serializeArray((Value.ArrayValue) value, buffer);
                break;
            case MAP:
                Serializers.serializeMap((Value.MapValue) value, buffer);
                break;
            case ROW:
                // Keep existing nested record serialization
                Value.RowValue rowValue = (Value.RowValue) value;
                var serializedRow = rowValue.getValue().serializeToBuffer();
                buffer.put(serializedRow);
                break;
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + typeCode);
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

        // Write directory directly to final buffer
        ImprintRecord.writeDirectoryToBuffer(sortedKeys, sortedValues, offsets, fieldCount, finalBuffer);

        // Write payload
        finalBuffer.put(payload);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }

    /**
     * Direct serializers that avoid virtual dispatch overhead.
     */
    static class Serializers {
        static void serializeBool(Value.BoolValue value, ByteBuffer buffer) {
            buffer.put((byte) (value.getValue() ? 1 : 0));
        }

        static void serializeInt32(Value.Int32Value value, ByteBuffer buffer) {
            buffer.putInt(value.getValue());
        }

        static void serializeInt64(Value.Int64Value value, ByteBuffer buffer) {
            buffer.putLong(value.getValue());
        }

        static void serializeFloat32(Value.Float32Value value, ByteBuffer buffer) {
            buffer.putFloat(value.getValue());
        }

        static void serializeFloat64(Value.Float64Value value, ByteBuffer buffer) {
            buffer.putDouble(value.getValue());
        }

        static void serializeString(Value value, ByteBuffer buffer) {
            if (value instanceof Value.StringValue) {
                var stringValue = (Value.StringValue) value;
                var utf8Bytes = stringValue.getUtf8Bytes(); // Already cached!
                VarInt.encode(utf8Bytes.length, buffer);
                buffer.put(utf8Bytes);
            } else {
                var bufferValue = (Value.StringBufferValue) value;
                var stringBuffer = bufferValue.getBuffer();
                VarInt.encode(stringBuffer.remaining(), buffer);
                buffer.put(stringBuffer);
            }
        }

        static void serializeBytes(Value value, ByteBuffer buffer) {
            if (value instanceof Value.BytesBufferValue) {
                var bufferValue = (Value.BytesBufferValue) value;
                var bytesBuffer = bufferValue.getBuffer();
                VarInt.encode(bytesBuffer.remaining(), buffer);
                buffer.put(bytesBuffer);
            } else {
                var bytesValue = (Value.BytesValue) value;
                byte[] bytes = bytesValue.getValue();
                VarInt.encode(bytes.length, buffer);
                buffer.put(bytes);
            }
        }

        static void serializeArray(Value.ArrayValue value, ByteBuffer buffer) throws ImprintException {
            var elements = value.getValue();
            VarInt.encode(elements.size(), buffer);

            if (elements.isEmpty()) return;

            var elementType = elements.get(0).getTypeCode();
            buffer.put(elementType.getCode());

            for (var element : elements) {
                if (element.getTypeCode() != elementType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR,
                            "Array elements must have same type code: " +
                                    element.getTypeCode() + " != " + elementType);
                }
                // Recursive call to serialize each element
                serializeValueByType(element, elementType, buffer);
            }
        }

        static void serializeMap(Value.MapValue value, ByteBuffer buffer) throws ImprintException {
            var map = value.getValue();
            VarInt.encode(map.size(), buffer);

            if (map.isEmpty()) return;

            var iterator = map.entrySet().iterator();
            var first = iterator.next();
            var keyType = first.getKey().getTypeCode();
            var valueType = first.getValue().getTypeCode();

            buffer.put(keyType.getCode());
            buffer.put(valueType.getCode());

            serializeMapKey(first.getKey(), buffer);
            serializeValueByType(first.getValue(), valueType, buffer);

            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.getKey().getTypeCode() != keyType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR,
                            "Map keys must have same type code: " +
                                    entry.getKey().getTypeCode() + " != " + keyType);
                }
                if (entry.getValue().getTypeCode() != valueType) {
                    throw new ImprintException(ErrorType.SCHEMA_ERROR,
                            "Map values must have same type code: " +
                                    entry.getValue().getTypeCode() + " != " + valueType);
                }

                serializeMapKey(entry.getKey(), buffer);
                serializeValueByType(entry.getValue(), valueType, buffer);
            }
        }

        // Helper method to avoid infinite recursion
        private static void serializeValueByType(Value value, TypeCode typeCode, ByteBuffer buffer) throws ImprintException {
            switch (typeCode) {
                case NULL:
                    break;
                case BOOL:
                    serializeBool((Value.BoolValue) value, buffer);
                    break;
                case INT32:
                    serializeInt32((Value.Int32Value) value, buffer);
                    break;
                case INT64:
                    serializeInt64((Value.Int64Value) value, buffer);
                    break;
                case FLOAT32:
                    serializeFloat32((Value.Float32Value) value, buffer);
                    break;
                case FLOAT64:
                    serializeFloat64((Value.Float64Value) value, buffer);
                    break;
                case STRING:
                    serializeString(value, buffer);
                    break;
                case BYTES:
                    serializeBytes(value, buffer);
                    break;
                case ARRAY:
                    serializeArray((Value.ArrayValue) value, buffer);
                    break;
                case MAP:
                    serializeMap((Value.MapValue) value, buffer);
                    break;
                default:
                    throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + typeCode);
            }
        }

        private static void serializeMapKey(MapKey key, ByteBuffer buffer) throws ImprintException {
            switch (key.getTypeCode()) {
                case INT32:
                    MapKey.Int32Key int32Key = (MapKey.Int32Key) key;
                    buffer.putInt(int32Key.getValue());
                    break;

                case INT64:
                    MapKey.Int64Key int64Key = (MapKey.Int64Key) key;
                    buffer.putLong(int64Key.getValue());
                    break;

                case BYTES:
                    MapKey.BytesKey bytesKey = (MapKey.BytesKey) key;
                    byte[] bytes = bytesKey.getValue();
                    VarInt.encode(bytes.length, buffer);
                    buffer.put(bytes);
                    break;

                case STRING:
                    MapKey.StringKey stringKey = (MapKey.StringKey) key;
                    byte[] stringBytes = stringKey.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    VarInt.encode(stringBytes.length, buffer);
                    buffer.put(stringBytes);
                    break;

                default:
                    throw new ImprintException(ErrorType.SERIALIZATION_ERROR,
                            "Invalid map key type: " + key.getTypeCode());
            }
        }
    }
}
