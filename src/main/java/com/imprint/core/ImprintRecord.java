package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.TypeCode;
import com.imprint.types.Value;
import com.imprint.util.VarInt;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * An Imprint record containing a header, field directory, and payload.
 * Uses ByteBuffer for zero-copy operations to achieve low latency.
 *
 * <p><strong>Performance Note:</strong> All ByteBuffers should be array-backed
 * (hasArray() == true) for optimal zero-copy performance. Direct buffers
 * may cause performance degradation.</p>
 */
@Getter
public final class ImprintRecord {
    private final Header header;
    private final List<DirectoryEntry> directory;
    private final ByteBuffer payload; // Read-only view for zero-copy

    /**
     * Creates a new ImprintRecord.
     *
     * @param payload the payload buffer. Should be array-backed for optimal performance.
     */
    public ImprintRecord(Header header, List<DirectoryEntry> directory, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.directory = List.copyOf(Objects.requireNonNull(directory, "Directory cannot be null"));
        this.payload = payload.asReadOnlyBuffer(); // Zero-copy read-only view
    }

    /**
     * Get a value by field ID, deserializing it on demand.
     * Returns null if the field is not found.
     * Note: If the field exists and is an explicit NULL type, this will return Value.NullValue.INSTANCE.
     */
    public Value getValue(int fieldId) throws ImprintException {
        var fieldBuffer = getFieldBuffer(fieldId);
        if (fieldBuffer == null) {
            // If getFieldBuffer returns null, it means the fieldId was not in the directory,
            // or an issue occurred slicing the payload (e.g., bad offsets).
            return null;
        }

        // findDirectoryIndex should not be negative here if fieldBuffer is not null,
        // but a defensive check or ensuring findDirectoryIndex is called once is good.
        // For simplicity, assume getFieldBuffer implies a valid index.
        int directoryIndex = findDirectoryIndex(fieldId);
        if (directoryIndex < 0) {
            // This case should ideally be caught by getFieldBuffer returning null.
            // If it happens, indicates an inconsistency.
            throw new ImprintException(ErrorType.INTERNAL_ERROR, "Field ID " + fieldId + " found buffer but not in directory.");
        }
        var entry = directory.get(directoryIndex);
        return deserializeValue(entry.getTypeCode(), fieldBuffer);
    }

    /**
     * Get the raw bytes for a field without deserializing.
     * Returns a zero-copy ByteBuffer view, or null if field not found.
     */
    public ByteBuffer getRawBytes(int fieldId) {
        var fieldBuffer = getFieldBuffer(fieldId);
        return fieldBuffer != null ? fieldBuffer.asReadOnlyBuffer() : null;
    }

    /**
     * Get a ByteBuffer view of a field's data.
     * Returns null if the field is not found.
     */
    private ByteBuffer getFieldBuffer(int fieldId) {
        int index = findDirectoryIndex(fieldId);
        if (index < 0) return null;

        var entry = directory.get(index);
        int startOffset = entry.getOffset();
        int endOffset = (index + 1 < directory.size()) ?
                directory.get(index + 1).getOffset() : payload.limit();

        var fieldBuffer = payload.duplicate();
        if (startOffset > payload.limit() || endOffset > payload.limit() || startOffset > endOffset) {
            return null;
        }
        fieldBuffer.position(startOffset).limit(endOffset);
        return fieldBuffer.slice();
    }

    /**
     * Serialize this record to a ByteBuffer.
     * The returned buffer will be array-backed.
     */
    public ByteBuffer serializeToBuffer() {
        var buffer = ByteBuffer.allocate(estimateSerializedSize());
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write header
        serializeHeader(buffer);

        // Write directory (always present)
        VarInt.encode(directory.size(), buffer);
        for (var entry : directory) {
            serializeDirectoryEntry(entry, buffer);
        }

        // Write payload (shallow copy only)
        var payloadCopy = payload.duplicate();
        buffer.put(payloadCopy);

        // Prepare buffer for reading
        buffer.flip();
        return buffer;
    }

    /**
     * Create a fluent builder for constructing ImprintRecord instances.
     *
     * @param schemaId the schema identifier for this record
     * @return a new builder instance
     */
    public static ImprintRecordBuilder builder(SchemaId schemaId) {
        return new ImprintRecordBuilder(schemaId);
    }

    /**
     * Create a fluent builder for constructing ImprintRecord instances.
     *
     * @param fieldspaceId the fieldspace identifier
     * @param schemaHash the schema hash
     * @return a new builder instance
     */
    @SuppressWarnings("unused")
    public static ImprintRecordBuilder builder(int fieldspaceId, int schemaHash) {
        return new ImprintRecordBuilder(new SchemaId(fieldspaceId, schemaHash));
    }

    /**
     * Deserialize a record from bytes through an array backed ByteBuffer.
     */
    public static ImprintRecord deserialize(byte[] bytes) throws ImprintException {
        return deserialize(ByteBuffer.wrap(bytes));
    }

    /**
     * Deserialize a record from a ByteBuffer.
     *
     * @param buffer the buffer to deserialize from. Must be array-backed
     *               (buffer.hasArray() == true) for optimal zero-copy performance.
     */
    public static ImprintRecord deserialize(ByteBuffer buffer) throws ImprintException {
        buffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // Read header
        var header = deserializeHeader(buffer);

        // Read directory (always present)
        var directory = new ArrayList<DirectoryEntry>();
        VarInt.DecodeResult countResult = VarInt.decode(buffer);
        int directoryCount = countResult.getValue();

        for (int i = 0; i < directoryCount; i++) {
            directory.add(deserializeDirectoryEntry(buffer));
        }

        // Read payload as ByteBuffer slice for zero-copy
        var payload = buffer.slice();
        payload.limit(header.getPayloadSize());
        buffer.position(buffer.position() + header.getPayloadSize());

        return new ImprintRecord(header, directory, payload);
    }

    /**
     * Binary search for field ID in directory without object allocation.
     * Returns the index of the field if found, or a negative value if not found.
     *
     * @param fieldId the field ID to search for
     * @return index if found, or negative insertion point - 1 if not found
     */
    private int findDirectoryIndex(int fieldId) {
        int low = 0;
        int high = directory.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1; // unsigned right shift to avoid overflow
            int midFieldId = directory.get(mid).getId();

            if (midFieldId < fieldId) {
                low = mid + 1;
            } else if (midFieldId > fieldId) {
                high = mid - 1;
            } else {
                return mid; // field found
            }
        }
        return -(low + 1); // field not found, return insertion point
    }

    public int estimateSerializedSize() {
        int size = Constants.HEADER_BYTES; // header
        size += VarInt.encodedLength(directory.size()); // directory count
        size += directory.size() * Constants.DIR_ENTRY_BYTES; // directory entries
        size += payload.remaining(); // payload
        return size;
    }

    private void serializeHeader(ByteBuffer buffer) {
        buffer.put(Constants.MAGIC);
        buffer.put(Constants.VERSION);
        buffer.put(header.getFlags().getValue());
        buffer.putInt(header.getSchemaId().getFieldSpaceId());
        buffer.putInt(header.getSchemaId().getSchemaHash());
        buffer.putInt(header.getPayloadSize());
    }

    private static Header deserializeHeader(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.HEADER_BYTES) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                    "Not enough bytes for header");
        }

        byte magic = buffer.get();
        if (magic != Constants.MAGIC) {
            throw new ImprintException(ErrorType.INVALID_MAGIC,
                    "Invalid magic byte: expected 0x" + Integer.toHexString(Constants.MAGIC) +
                            ", got 0x" + Integer.toHexString(magic & 0xFF));
        }

        byte version = buffer.get();
        if (version != Constants.VERSION) {
            throw new ImprintException(ErrorType.UNSUPPORTED_VERSION,
                    "Unsupported version: " + version);
        }

        var flags = new Flags(buffer.get());
        int fieldspaceId = buffer.getInt();
        int schemaHash = buffer.getInt();
        int payloadSize = buffer.getInt();

        return new Header(flags, new SchemaId(fieldspaceId, schemaHash), payloadSize);
    }

    private void serializeDirectoryEntry(DirectoryEntry entry, ByteBuffer buffer) {
        buffer.putShort(entry.getId());
        buffer.put(entry.getTypeCode().getCode());
        buffer.putInt(entry.getOffset());
    }

    private static DirectoryEntry deserializeDirectoryEntry(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.DIR_ENTRY_BYTES) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                    "Not enough bytes for directory entry");
        }

        short id = buffer.getShort();
        var typeCode = TypeCode.fromByte(buffer.get());
        int offset = buffer.getInt();

        return new DirectoryEntry(id, typeCode, offset);
    }

    private Value deserializeValue(TypeCode typeCode, ByteBuffer buffer) throws ImprintException {
        ByteBuffer valueSpecificBuffer = buffer.duplicate();
        valueSpecificBuffer.order(ByteOrder.LITTLE_ENDIAN);

        switch (typeCode) {
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
                return typeCode.getHandler().deserialize(valueSpecificBuffer);
            case ROW:
                var nestedRecord = deserialize(valueSpecificBuffer);
                return Value.fromRow(nestedRecord);

            default:
                throw new ImprintException(ErrorType.INVALID_TYPE_CODE, "Unknown type code: " + typeCode);
        }
    }

    @Override
    public String toString() {
        return String.format("ImprintRecord{header=%s, directorySize=%d, payloadSize=%d}",
                header, directory.size(), payload.remaining());
    }

    private <T extends Value> T getTypedValueOrThrow(int fieldId, TypeCode expectedTypeCode, Class<T> expectedValueClass, String expectedTypeName) throws ImprintException {
        Value value = getValue(fieldId);

        if (value == null) {
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND,
                    "Field " + fieldId + " not found, cannot retrieve as " + expectedTypeName + ".");
        }

        if (value.getTypeCode() == TypeCode.NULL) {
            throw new ImprintException(ErrorType.TYPE_MISMATCH,
                    "Field " + fieldId + " is NULL, cannot retrieve as " + expectedTypeName + ".");
        }

        if (value.getTypeCode() == expectedTypeCode && expectedValueClass.isInstance(value)) {
            return expectedValueClass.cast(value);
        }

        throw new ImprintException(ErrorType.TYPE_MISMATCH,
                "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected " + expectedTypeName + ".");
    }

    /**
     * Retrieves the boolean value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type BOOL.
     */
    public boolean getBoolean(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.BOOL, Value.BoolValue.class, "boolean").getValue();
    }

    /**
     * Retrieves the int (int32) value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type INT32.
     */
    public int getInt32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.INT32, Value.Int32Value.class, "int32").getValue();
    }

    /**
     * Retrieves the long (int64) value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type INT64.
     */
    public long getInt64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.INT64, Value.Int64Value.class, "int64").getValue();
    }

    /**
     * Retrieves the float (float32) value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type FLOAT32.
     */
    public float getFloat32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.FLOAT32, Value.Float32Value.class, "float32").getValue();
    }

    /**
     * Retrieves the double (float64) value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type FLOAT64.
     */
    public double getFloat64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.FLOAT64, Value.Float64Value.class, "float64").getValue();
    }

    /**
     * Retrieves the String value for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type STRING.
     */
    public String getString(int fieldId) throws ImprintException {
        Value value = getValue(fieldId);

        if (value == null) {
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND,
                    "Field " + fieldId + " not found, cannot retrieve as String.");
        }
        if (value.getTypeCode() == TypeCode.NULL) {
            throw new ImprintException(ErrorType.TYPE_MISMATCH,
                    "Field " + fieldId + " is NULL, cannot retrieve as String.");
        }

        if (value instanceof Value.StringValue) {
            return ((Value.StringValue) value).getValue();
        }
        if (value instanceof Value.StringBufferValue) {
            return ((Value.StringBufferValue) value).getValue();
        }

        throw new ImprintException(ErrorType.TYPE_MISMATCH,
                "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected STRING.");
    }

    /**
     * Retrieves the byte array (byte[]) value for the given field ID.
     * Note: This may involve a defensive copy depending on the underlying Value type.
     * @throws ImprintException if the field is not found, is null, or is not of type BYTES.
     */
    public byte[] getBytes(int fieldId) throws ImprintException {
        Value value = getValue(fieldId);

        if (value == null) {
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND,
                    "Field " + fieldId + " not found, cannot retrieve as byte[].");
        }
        if (value.getTypeCode() == TypeCode.NULL) {
            throw new ImprintException(ErrorType.TYPE_MISMATCH,
                    "Field " + fieldId + " is NULL, cannot retrieve as byte[].");
        }

        if (value instanceof Value.BytesValue) {
            return ((Value.BytesValue) value).getValue(); // getValue() in BytesValue returns a clone
        }
        if (value instanceof Value.BytesBufferValue) {
            return ((Value.BytesBufferValue) value).getValue(); // getValue() in BytesBufferValue creates a new array
        }

        throw new ImprintException(ErrorType.TYPE_MISMATCH,
                "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected BYTES.");
    }

    /**
     * Retrieves the List<Value> for the given field ID.
     * The list itself is a copy; modifications to it will not affect the record.
     * @throws ImprintException if the field is not found, is null, or is not of type ARRAY.
     */
    public List<Value> getArray(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.ARRAY, Value.ArrayValue.class, "ARRAY").getValue();
    }

    /**
     * Retrieves the Map<MapKey, Value> for the given field ID.
     * The map itself is a copy; modifications to it will not affect the record.
     * @throws ImprintException if the field is not found, is null, or is not of type MAP.
     */
    public Map<MapKey, Value> getMap(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.MAP, Value.MapValue.class, "MAP").getValue();
    }

    /**
     * Retrieves the nested ImprintRecord for the given field ID.
     * @throws ImprintException if the field is not found, is null, or is not of type ROW.
     */
    public ImprintRecord getRow(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.ROW, Value.RowValue.class, "ROW").getValue();
    }
}