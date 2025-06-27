package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.ImprintSerializers;
import com.imprint.types.MapKey;
import com.imprint.types.TypeCode;
import com.imprint.util.ImprintBuffer;
import com.imprint.util.VarInt;
import lombok.SneakyThrows;
import lombok.Value;

import java.nio.ByteBuffer;
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


    @Value
    static class FieldValue {
        byte typeCode;
        Object value;
        
        // Factory methods for primitives
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

    // Collections - store as raw collections for now, convert during serialization
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


    // Build the final record
    public ImprintRecord build() throws ImprintException {
        // Build to bytes and then create ImprintRecord from bytes for consistency
        var buffer = buildToBuffer();
        return ImprintRecord.fromBytes(buffer);
    }

    /**
     * Builds the record and serializes it directly to a ByteBuffer using growable buffer optimization.
     *
     * @return A read-only ByteBuffer containing the fully serialized record.
     * @throws ImprintException if serialization fails.
     */
    public ImprintBuffer buildToBuffer() throws ImprintException {
        // 1. Calculate conservative size BEFORE sorting (which invalidates the map)
        int conservativeSize = calculateConservativePayloadSize();

        // 2. Sort fields by ID for directory ordering
        var sortedFieldsResult = getSortedFieldsResult();
        var sortedValues = sortedFieldsResult.getValues();
        var sortedKeys = sortedFieldsResult.getKeys();
        var fieldCount = sortedFieldsResult.getCount();

        // 3. Calculate directory size
        int directorySize = ImprintRecord.calculateDirectorySize(fieldCount);
        
        // Debug: Log directory size calculation for empty records
        if (fieldCount == 0) {
            System.err.println("DEBUG: Directory size calculation for empty record:");
            System.err.println("  fieldCount=" + fieldCount);
            System.err.println("  VarInt.encodedLength(" + fieldCount + ")=" + com.imprint.util.VarInt.encodedLength(fieldCount));
            System.err.println("  calculated directorySize=" + directorySize);
        }
        
        // 4. Use growable buffer to eliminate size guessing and retry logic
        return serializeToBuffer(schemaId, sortedKeys, sortedValues, fieldCount,
            conservativeSize, directorySize);
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

    /**
     * Fast field size estimation using heuristics for performance.
     */
    @SneakyThrows
    private int estimateFieldSize(FieldValue fieldValue) {
        var typeCode = TypeCode.fromByte(fieldValue.typeCode);
        return ImprintSerializers.estimateSize(typeCode, fieldValue.value);
    }

    /**
     * Get current estimated payload size with 25% buffer.
     */
    private int calculateConservativePayloadSize() {
        // Add 25% buffer for safety margin
        return Math.max(estimatedPayloadSize + (estimatedPayloadSize / 4), 4096);
    }



    /**
     * Serialize using growable buffer - eliminates size guessing and retry logic.
     * Uses growable buffer that automatically expands as needed during serialization.
     * //TODO: we have multiple places where we write header/directory and we should probably consolidate that
     */
    private ImprintBuffer serializeToBuffer(SchemaId schemaId, short[] sortedKeys, Object[] sortedValues,
                                         int fieldCount, int conservativePayloadSize, int directorySize) throws ImprintException {
        
        // Start with conservative estimate, use fixed size buffer first
        int initialSize = Constants.HEADER_BYTES + directorySize + conservativePayloadSize;
        var buffer = new ImprintBuffer(new byte[initialSize * 2]); // Extra capacity to avoid growth
        
        // Reserve space for header and directory - write payload first
        int headerAndDirSize = Constants.HEADER_BYTES + directorySize;
        buffer.position(headerAndDirSize);
        
        // Serialize payload and collect offsets - buffer will grow automatically
        int[] offsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            var fieldValue = (FieldValue) sortedValues[i];
            offsets[i] = buffer.position() - headerAndDirSize; // Offset relative to payload start
            serializeFieldValue(fieldValue, buffer);
        }
        
        int actualPayloadSize = buffer.position() - headerAndDirSize;
        
        // Now write header and directory at the beginning
        buffer.position(0);
        
        // Write header with actual payload size
        buffer.putByte(Constants.MAGIC);
        buffer.putByte(Constants.VERSION);
        buffer.putByte((byte) 0); // flags
        buffer.putInt(schemaId.getFieldSpaceId());
        buffer.putInt(schemaId.getSchemaHash());
        buffer.putInt(actualPayloadSize);
        
        // Write directory
        writeDirectoryToBuffer(sortedKeys, sortedValues, offsets, fieldCount, buffer);
        
        // Set final limit and prepare for reading
        int finalSize = Constants.HEADER_BYTES + directorySize + actualPayloadSize;
        
        // Debug: Log buffer creation details for empty records
        if (fieldCount == 0) {
            System.err.println("DEBUG: Empty record serialization details:");
            System.err.println("  fieldCount=" + fieldCount);
            System.err.println("  directorySize=" + directorySize);
            System.err.println("  actualPayloadSize=" + actualPayloadSize);
            System.err.println("  finalSize=" + finalSize);
            System.err.println("  Constants.HEADER_BYTES=" + Constants.HEADER_BYTES);
        }
        
        // Ensure minimum buffer size for validation (at least header size)
        if (finalSize < Constants.HEADER_BYTES) {
            throw new IllegalStateException("Buffer size (" + finalSize + ") is smaller than minimum header size (" + Constants.HEADER_BYTES + ")");
        }
        
        buffer.position(0);
        buffer.limit(finalSize);
        
        return buffer.asReadOnlyBuffer();
    }

    /**
     * Legacy serialization method with retry logic - kept for compatibility.
     * Serialize directly to a single buffer with conservative size multiplier.
     * Writes payload first, then backfills header and directory.
     */
    private ByteBuffer serializeToSingleBuffer(SchemaId schemaId, short[] sortedKeys, Object[] sortedValues, int fieldCount, int conservativePayloadSize, int directorySize,
                                               int sizeMultiplier) throws ImprintException {
        
        // Calculate total buffer size needed
        int totalSize = Constants.HEADER_BYTES + directorySize + (conservativePayloadSize * sizeMultiplier);
        byte[] bufferArray = new byte[totalSize];
        var buffer = new ImprintBuffer(bufferArray);
        
        // Skip header and directory space, write payload first
        int payloadStart = Constants.HEADER_BYTES + directorySize;
        buffer.position(payloadStart);
        
        // Serialize payload and collect offsets
        int[] offsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            var fieldValue = (FieldValue) sortedValues[i];
            offsets[i] = buffer.position() - payloadStart; // Offset relative to payload start
            serializeFieldValue(fieldValue, buffer);
        }
        
        int actualPayloadSize = buffer.position() - payloadStart;
        
        // Return to the front to fill in the header now that we have true payload size
        buffer.position(0);
        
        // Write header
        var header = new Header(new Flags((byte) 0), schemaId, actualPayloadSize);
        buffer.putByte(Constants.MAGIC);
        buffer.putByte(Constants.VERSION);
        buffer.putByte(header.getFlags().getValue());
        buffer.putInt(header.getSchemaId().getFieldSpaceId());
        buffer.putInt(header.getSchemaId().getSchemaHash());
        buffer.putInt(header.getPayloadSize());
        
        // Write directory
        writeDirectoryToBuffer(sortedKeys, sortedValues, offsets, fieldCount, buffer);
        
        // Set final limit and flip
        int finalSize = Constants.HEADER_BYTES + directorySize + actualPayloadSize;
        buffer.limit(finalSize);
        buffer.position(0);
        
        return buffer.toByteBuffer().asReadOnlyBuffer();
    }

    private void serializeFieldValue(FieldValue fieldValue, ImprintBuffer buffer) throws ImprintException {
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
                // Nested record serialization
                var nestedRecord = (ImprintRecord) value;
                var serializedRow = nestedRecord.serializeToBuffer();
                // Copy data from read-only ByteBuffer to byte array first
                byte[] rowBytes = new byte[serializedRow.remaining()];
                serializedRow.get(rowBytes);
                buffer.putBytes(rowBytes);
                break;
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, "Unknown type code: " + typeCode);
        }
    }

    //TODO kinda hacky here, arrays and maps definitely need some functional updates to the flow
    private void serializeArray(List<?> list, ImprintBuffer buffer) throws ImprintException {
        ImprintSerializers.serializeArray(list, buffer,
            this::getTypeCodeForObject, 
            this::serializeObjectDirect);
    }
    
    private void serializeMap(Map<?, ?> map, ImprintBuffer buffer) throws ImprintException {
        ImprintSerializers.serializeMap(map, buffer,
            this::convertToMapKey,
            this::getTypeCodeForObject,
            this::serializeObjectDirect);
    }
    
    // Helper methods for static serializers
    private TypeCode getTypeCodeForObject(Object obj) {
        var fieldValue = convertToFieldValue(obj);
        try {
            return TypeCode.fromByte(fieldValue.typeCode);
        } catch (ImprintException e) {
            throw new RuntimeException("Invalid type code", e);
        }
    }
    
    private void serializeObjectDirect(Object obj, ImprintBuffer buffer) {
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
     * Write directory entries directly to buffer for FieldValue objects.
     */
    private static void writeDirectoryToBuffer(short[] sortedKeys, Object[] sortedValues, int[] offsets, int fieldCount, ImprintBuffer buffer) {
        // Write field count using putVarInt for consistency with ImprintOperations
        buffer.putVarInt(fieldCount);

        // Early return for empty directory
        if (fieldCount == 0)
            return;
        for (int i = 0; i < fieldCount; i++) {
            var fieldValue = (FieldValue) sortedValues[i];
            // Write directory entry: field ID (2 bytes), type code (1 byte), offset (4 bytes)
            buffer.putShort(sortedKeys[i]);        // bytes 0-1: field ID
            buffer.putByte(fieldValue.typeCode);   // byte 2: type code
            buffer.putInt(offsets[i]);             // bytes 3-6: offset
        }
    }

}
