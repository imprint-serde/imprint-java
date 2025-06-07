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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An Imprint record containing a header and buffer management.
 * Delegates all buffer operations to ImprintBuffers for cleaner separation.
 */
@Getter
public final class ImprintRecord {
    private final Header header;
    private final ImprintBuffers buffers;

    /**
     * Creates a record from deserialized components.
     */
    private ImprintRecord(Header header, ImprintBuffers buffers) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.buffers = Objects.requireNonNull(buffers, "Buffers cannot be null");
    }

    /**
     * Creates a record from pre-parsed directory (used by ImprintWriter).
     */
    ImprintRecord(Header header, List<DirectoryEntry> directory, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.buffers = new ImprintBuffers(directory, payload);
    }

    /**
     * Creates a record from a pre-parsed and sorted directory map (used by ImprintRecordBuilder).
     */
    ImprintRecord(Header header, TreeMap<Integer, DirectoryEntry> directoryMap, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.buffers = new ImprintBuffers(directoryMap, payload);
    }

    // ========== FIELD ACCESS METHODS ==========

    /**
     * Get a value by field ID, deserializing it on demand.
     * Returns null if the field is not found.
     */
    public Value getValue(int fieldId) throws ImprintException {
        var entry = buffers.findDirectoryEntry(fieldId);
        if (entry == null)
            return null;

        var fieldBuffer = buffers.getFieldBuffer(fieldId);
        if (fieldBuffer == null)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Failed to get buffer for field " + fieldId);

        return deserializeValue(entry.getTypeCode(), fieldBuffer);
    }

    /**
     * Get raw bytes for a field without deserializing.
     */
    public ByteBuffer getRawBytes(int fieldId) {
        try {
            return buffers.getFieldBuffer(fieldId);
        } catch (ImprintException e) {
            return null;
        }
    }

    /**
     * Project a subset of fields from this record.
     *
     * @param fieldIds Array of field IDs to include in the projection
     * @return New ImprintRecord containing only the requested fields
     */
    public ImprintRecord project(int... fieldIds) {
        return ImprintOperations.project(this, fieldIds);
    }

    /**
     * Merge another record into this one.
     * For duplicate fields, this record's values take precedence.
     *
     * @param other The record to merge with this one
     * @return New ImprintRecord containing merged fields
     * @throws ImprintException if merge fails
     */
    public ImprintRecord merge(ImprintRecord other) throws ImprintException {
        return ImprintOperations.merge(this, other);
    }

    /**
     * Get the directory (parsing it if necessary).
     */
    public List<DirectoryEntry> getDirectory() {
        return buffers.getDirectory();
    }

    // ========== TYPED GETTERS ==========

    public boolean getBoolean(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.BOOL, Value.BoolValue.class, "boolean").getValue();
    }

    public int getInt32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.INT32, Value.Int32Value.class, "int32").getValue();
    }

    public long getInt64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.INT64, Value.Int64Value.class, "int64").getValue();
    }

    public float getFloat32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.FLOAT32, Value.Float32Value.class, "float32").getValue();
    }

    public double getFloat64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.FLOAT64, Value.Float64Value.class, "float64").getValue();
    }

    public String getString(int fieldId) throws ImprintException {
        var value = getValidatedValue(fieldId, "STRING");
        if (value instanceof Value.StringValue)
            return ((Value.StringValue) value).getValue();
        if (value instanceof Value.StringBufferValue)
            return ((Value.StringBufferValue) value).getValue();
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is not a STRING");
    }

    public byte[] getBytes(int fieldId) throws ImprintException {
        var value = getValidatedValue(fieldId, "BYTES");
        if (value instanceof Value.BytesValue)
            return ((Value.BytesValue) value).getValue();
        if (value instanceof Value.BytesBufferValue)
            return ((Value.BytesBufferValue) value).getValue();
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is not BYTES");
    }

    public List<Value> getArray(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.ARRAY, Value.ArrayValue.class, "ARRAY").getValue();
    }

    public Map<MapKey, Value> getMap(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.MAP, Value.MapValue.class, "MAP").getValue();
    }

    public ImprintRecord getRow(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, TypeCode.ROW, Value.RowValue.class, "ROW").getValue();
    }

    // ========== SERIALIZATION ==========

    /**
     * Serialize this record to a ByteBuffer.
     */
    public ByteBuffer serializeToBuffer() {
        var directoryBuffer = buffers.serializeDirectory(); // This is now optimized to return a duplicate
        var payloadBuffer = buffers.getPayload();

        int finalSize = Constants.HEADER_BYTES + directoryBuffer.remaining() + payloadBuffer.remaining();
        var finalBuffer = ByteBuffer.allocate(finalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Assemble the final record from existing components
        serializeHeader(this.header, finalBuffer);
        finalBuffer.put(directoryBuffer);
        finalBuffer.put(payloadBuffer.duplicate()); // Use duplicate to preserve original buffer state

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }

    public int estimateSerializedSize() {
        int size = Constants.HEADER_BYTES; // header
        size += buffers.serializeDirectory().remaining(); // directory
        size += buffers.getPayload().remaining(); // payload
        return size;
    }

    /**
     * Serializes the components of a record into a single ByteBuffer.
     * This provides a direct serialization path without needing a live ImprintRecord instance.
     *
     * @param schemaId  The schema identifier for the record.
     * @param directory The list of directory entries, which must be sorted by field ID.
     * @param payload   The ByteBuffer containing all field data concatenated.
     * @return A read-only ByteBuffer with the complete serialized record.
     */
    public static ByteBuffer serialize(SchemaId schemaId, List<DirectoryEntry> directory, ByteBuffer payload) {
        var header = new Header(new Flags((byte) 0), schemaId, payload.remaining());
        var directoryBuffer = ImprintBuffers.createDirectoryBuffer(directory);

        int finalSize = Constants.HEADER_BYTES + directoryBuffer.remaining() + payload.remaining();
        var finalBuffer = ByteBuffer.allocate(finalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Assemble the final record
        serializeHeader(header, finalBuffer);
        finalBuffer.put(directoryBuffer);
        finalBuffer.put(payload);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }

    /**
     * Serializes the components of a record into a single ByteBuffer using a pre-built directory map.
     * This provides a direct serialization path without needing a live ImprintRecord instance.
     *
     * @param schemaId     The schema identifier for the record.
     * @param directoryMap The map of directory entries, which must be sorted by field ID (e.g., a TreeMap).
     * @param payload      The ByteBuffer containing all field data concatenated.
     * @return A read-only ByteBuffer with the complete serialized record.
     */
    public static ByteBuffer serialize(SchemaId schemaId, TreeMap<Integer, DirectoryEntry> directoryMap, ByteBuffer payload) {
        var header = new Header(new Flags((byte) 0), schemaId, payload.remaining());
        var directoryBuffer = ImprintBuffers.createDirectoryBufferFromMap(directoryMap);

        int finalSize = Constants.HEADER_BYTES + directoryBuffer.remaining() + payload.remaining();
        var finalBuffer = ByteBuffer.allocate(finalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Assemble the final record
        serializeHeader(header, finalBuffer);
        finalBuffer.put(directoryBuffer);
        finalBuffer.put(payload);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }

    // ========== STATIC FACTORY METHODS ==========

    public static ImprintRecordBuilder builder(SchemaId schemaId) {
        return new ImprintRecordBuilder(schemaId);
    }

    public static ImprintRecordBuilder builder(int fieldspaceId, int schemaHash) {
        return new ImprintRecordBuilder(new SchemaId(fieldspaceId, schemaHash));
    }

    public static ImprintRecord deserialize(byte[] bytes) throws ImprintException {
        return deserialize(ByteBuffer.wrap(bytes));
    }

    public static ImprintRecord deserialize(ByteBuffer buffer) throws ImprintException {
        buffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // Read header
        var header = deserializeHeader(buffer);

        // Calculate directory size
        int directoryStartPos = buffer.position();
        var countResult = VarInt.decode(buffer);
        int directoryCount = countResult.getValue();
        int directorySize = countResult.getBytesRead() + (directoryCount * Constants.DIR_ENTRY_BYTES);

        // Create directory buffer
        buffer.position(directoryStartPos);
        var directoryBuffer = buffer.slice();
        directoryBuffer.limit(directorySize);

        // Advance past directory
        buffer.position(buffer.position() + directorySize);

        // Create payload buffer
        var payload = buffer.slice();
        payload.limit(header.getPayloadSize());

        // Create buffers wrapper
        var buffers = new ImprintBuffers(directoryBuffer, payload);

        return new ImprintRecord(header, buffers);
    }

    // ========== PRIVATE HELPER METHODS ==========

    /**
     * Get and validate a value exists and is not null.
     */
    private Value getValidatedValue(int fieldId, String typeName) throws ImprintException {
        var value = getValue(fieldId);
        if (value == null)
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND, "Field " + fieldId + " not found");
        if (value.getTypeCode() == TypeCode.NULL)
            throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is NULL, cannot retrieve as " + typeName);
        return value;
    }

    private <T extends Value> T getTypedValueOrThrow(int fieldId, TypeCode expectedTypeCode, Class<T> expectedValueClass, String expectedTypeName)
            throws ImprintException {
        var value = getValidatedValue(fieldId, expectedTypeName);
        if (value.getTypeCode() == expectedTypeCode && expectedValueClass.isInstance(value))
            return expectedValueClass.cast(value);
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected " + expectedTypeName);
    }

    private Value deserializeValue(TypeCode typeCode, ByteBuffer buffer) throws ImprintException {
        var valueBuffer = buffer.duplicate();
        valueBuffer.order(ByteOrder.LITTLE_ENDIAN);

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
                return typeCode.getHandler().deserialize(valueBuffer);
            case ROW:
                var nestedRecord = deserialize(valueBuffer);
                return Value.fromRow(nestedRecord);
            default:
                throw new ImprintException(ErrorType.INVALID_TYPE_CODE, "Unknown type code: " + typeCode);
        }
    }

    private static void serializeHeader(Header header, ByteBuffer buffer) {
        buffer.put(Constants.MAGIC);
        buffer.put(Constants.VERSION);
        buffer.put(header.getFlags().getValue());
        buffer.putInt(header.getSchemaId().getFieldSpaceId());
        buffer.putInt(header.getSchemaId().getSchemaHash());
        buffer.putInt(header.getPayloadSize());
    }

    private static Header deserializeHeader(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.HEADER_BYTES)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for header");


        byte magic = buffer.get();
        if (magic != Constants.MAGIC) {
            throw new ImprintException(ErrorType.INVALID_MAGIC, "Invalid magic byte: expected 0x" + Integer.toHexString(Constants.MAGIC) +
                            ", got 0x" + Integer.toHexString(magic & 0xFF));
        }

        byte version = buffer.get();
        if (version != Constants.VERSION) {
            throw new ImprintException(ErrorType.UNSUPPORTED_VERSION, "Unsupported version: " + version);
        }

        var flags = new Flags(buffer.get());
        int fieldSpaceId = buffer.getInt();
        int schemaHash = buffer.getInt();
        int payloadSize = buffer.getInt();

        return new Header(flags, new SchemaId(fieldSpaceId, schemaHash), payloadSize);
    }

    @Override
    public String toString() {
        return String.format("ImprintRecord{header=%s, directorySize=%d, payloadSize=%d}",
                header, buffers.getDirectoryCount(), buffers.getPayload().remaining());
    }
}