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
 * <p>This implementation uses lazy directory parsing for optimal single field access performance.
 * The directory is only parsed when needed, and binary search is performed directly on raw bytes
 * when possible.</p>
 *
 * <p><strong>Performance Note:</strong> All ByteBuffers should be array-backed
 * (hasArray() == true) for optimal zero-copy performance. Direct buffers
 * may cause performance degradation.</p>
 */
@Getter
public final class ImprintRecord {
    private final Header header;
    private final ByteBuffer directoryBuffer; // Raw directory bytes
    private final ByteBuffer payload; // Read-only view for zero-copy

    // Lazy-loaded directory state
    private List<DirectoryEntry> parsedDirectory;
    private boolean directoryParsed = false;

    // Cache for parsed directory count to avoid repeated VarInt decoding
    private int directoryCount = -1;

    /**
     * Creates a new ImprintRecord with lazy directory parsing.
     *
     * @param header the record header
     * @param directoryBuffer raw directory bytes (including count)
     * @param payload the payload buffer. Should be array-backed for optimal performance.
     */
    private ImprintRecord(Header header, ByteBuffer directoryBuffer, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.directoryBuffer = directoryBuffer.asReadOnlyBuffer();
        this.payload = payload.asReadOnlyBuffer(); // Zero-copy read-only view
    }

    /**
     * Creates a new ImprintRecord with pre-parsed directory (used by ImprintWriter).
     * This constructor is used when the directory is already known and parsed.
     *
     * @param header the record header
     * @param directory the parsed directory entries
     * @param payload the payload buffer. Should be array-backed for optimal performance.
     */
    ImprintRecord(Header header, List<DirectoryEntry> directory, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.parsedDirectory = Collections.unmodifiableList(Objects.requireNonNull(directory, "Directory cannot be null"));
        this.directoryParsed = true;
        this.directoryCount = directory.size();
        this.payload = payload.asReadOnlyBuffer();

        // Create directory buffer for serialization compatibility
        this.directoryBuffer = createDirectoryBuffer(directory);
    }

    /**
     * Get a value by field ID, deserializing it on demand.
     * Returns null if the field is not found.
     * Note: If the field exists and is an explicit NULL type, this will return Value.NullValue.INSTANCE
     *
     * <p><strong>Performance Note:</strong> Accessing fields one-by-one is optimized for single field access.
     * If you need to access many fields from the same record, consider calling getDirectory() first
     * to parse the full directory once, then access fields normally.</p>
     */
    public Value getValue(int fieldId) throws ImprintException {
        DirectoryEntry entry = findDirectoryEntry(fieldId);
        if (entry == null) {
            return null;
        }

        return deserializeValue(entry.getTypeCode(), getFieldBufferFromEntry(entry));
    }

    /**
     * Get the raw bytes for a field without deserializing.
     * Returns a zero-copy ByteBuffer view, or null if field not found.
     */
    public ByteBuffer getRawBytes(int fieldId) {
        try {
            DirectoryEntry entry = findDirectoryEntry(fieldId);
            if (entry == null) {
                return null;
            }

            return getFieldBufferFromEntry(entry).asReadOnlyBuffer();
        } catch (ImprintException e) {
            return null;
        }
    }

    /**
     * Find a directory entry for the given field ID.
     * Uses the most efficient method based on current state.
     */
    private DirectoryEntry findDirectoryEntry(int fieldId) throws ImprintException {
        if (directoryParsed) {
            // Use parsed directory
            int index = findDirectoryIndexInParsed(fieldId);
            return index >= 0 ? parsedDirectory.get(index) : null;
        } else {
            // Use fast binary search on raw bytes
            return findFieldEntryFast(fieldId);
        }
    }

    /**
     * Fast binary search directly on raw directory bytes.
     * This avoids parsing the entire directory for single field access.
     */
    private DirectoryEntry findFieldEntryFast(int fieldId) throws ImprintException {
        ByteBuffer searchBuffer = directoryBuffer.duplicate();
        searchBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Decode directory count (cache it to avoid repeated decoding)
        if (directoryCount < 0) {
            directoryCount = VarInt.decode(searchBuffer).getValue();
        } else {
            // Skip past the VarInt count
            VarInt.decode(searchBuffer);
        }

        if (directoryCount == 0) {
            return null;
        }

        // Now searchBuffer.position() points to the first directory entry
        int directoryStartPos = searchBuffer.position();

        int low = 0;
        int high = directoryCount - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            // Calculate position of mid entry
            int entryPos = directoryStartPos + (mid * Constants.DIR_ENTRY_BYTES);

            // Bounds check
            if (entryPos + Constants.DIR_ENTRY_BYTES > searchBuffer.limit()) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                        "Directory entry at position " + entryPos + " exceeds buffer limit " + searchBuffer.limit());
            }

            searchBuffer.position(entryPos);
            short midFieldId = searchBuffer.getShort();

            if (midFieldId < fieldId) {
                low = mid + 1;
            } else if (midFieldId > fieldId) {
                high = mid - 1;
            } else {
                // Found it - read the complete entry
                searchBuffer.position(entryPos);
                return deserializeDirectoryEntry(searchBuffer);
            }
        }

        return null; // Not found
    }

    /**
     * Get the directory (parsing it if necessary).
     * This maintains backward compatibility with existing code.
     *
     * <p><strong>Performance Tip:</strong> If you plan to access many fields from this record,
     * call this method first to parse the directory once, then use the field accessor methods.
     * This is more efficient than accessing fields one-by-one when you need multiple fields.</p>
     */
    public List<DirectoryEntry> getDirectory() {
        ensureDirectoryParsed();
        return parsedDirectory;
    }

    /**
     * Get a ByteBuffer view of a field's data from a DirectoryEntry.
     */
    private ByteBuffer getFieldBufferFromEntry(DirectoryEntry entry) throws ImprintException {
        int startOffset = entry.getOffset();

        // Find end offset
        int endOffset;
        if (directoryParsed) {
            // Use parsed directory to find next entry
            int entryIndex = findDirectoryIndexInParsed(entry.getId());
            endOffset = (entryIndex + 1 < parsedDirectory.size()) ?
                    parsedDirectory.get(entryIndex + 1).getOffset() : payload.limit();
        } else {
            // Calculate end offset by finding the next field in the directory
            endOffset = findNextOffsetInRawDirectory(entry.getId());
        }

        if (startOffset < 0 || endOffset < 0 || startOffset > payload.limit() ||
                endOffset > payload.limit() || startOffset > endOffset) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                    "Invalid field buffer range: start=" + startOffset + ", end=" + endOffset +
                            ", payloadLimit=" + payload.limit());
        }

        var fieldBuffer = payload.duplicate();
        fieldBuffer.position(startOffset).limit(endOffset);
        return fieldBuffer;
    }

    /**
     * Find the next field's offset by scanning the raw directory.
     * This is used when the directory isn't fully parsed yet.
     */
    private int findNextOffsetInRawDirectory(int currentFieldId) throws ImprintException {
        ByteBuffer scanBuffer = directoryBuffer.duplicate();
        scanBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Get directory count
        int count = (directoryCount >= 0) ? directoryCount : VarInt.decode(scanBuffer).getValue();
        if (count == 0) {
            return payload.limit();
        }

        // Skip past count if we just decoded it
        if (directoryCount < 0) {
            // VarInt.decode already advanced the position
        } else {
            VarInt.decode(scanBuffer); // Skip past the count
        }

        int directoryStartPos = scanBuffer.position();

        for (int i = 0; i < count; i++) {
            int entryPos = directoryStartPos + (i * Constants.DIR_ENTRY_BYTES);

            // Bounds check
            if (entryPos + Constants.DIR_ENTRY_BYTES > scanBuffer.limit()) {
                return payload.limit();
            }

            scanBuffer.position(entryPos);
            short fieldId = scanBuffer.getShort();
            scanBuffer.get(); // skip type
            int offset = scanBuffer.getInt();

            if (fieldId > currentFieldId) {
                return offset; // Found next field's offset
            }
        }

        return payload.limit(); // No next field, use payload end
    }

    /**
     * Ensure the directory is fully parsed (thread-safe).
     */
    private synchronized void ensureDirectoryParsed() {
        if (directoryParsed) {
            return;
        }

        try {
            ByteBuffer parseBuffer = directoryBuffer.duplicate();
            parseBuffer.order(ByteOrder.LITTLE_ENDIAN);

            VarInt.DecodeResult countResult = VarInt.decode(parseBuffer);
            int count = countResult.getValue();
            this.directoryCount = count; // Cache the count

            List<DirectoryEntry> directory = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                directory.add(deserializeDirectoryEntry(parseBuffer));
            }

            this.parsedDirectory = Collections.unmodifiableList(directory);
            this.directoryParsed = true;
        } catch (ImprintException e) {
            throw new RuntimeException("Failed to parse directory", e);
        }
    }

    /**
     * Creates a directory buffer from parsed directory entries.
     * This is used when creating records with pre-parsed directories (e.g., from ImprintWriter).
     */
    private ByteBuffer createDirectoryBuffer(List<DirectoryEntry> directory) {
        try {
            int bufferSize = VarInt.encodedLength(directory.size()) + (directory.size() * Constants.DIR_ENTRY_BYTES);
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // Write directory count
            VarInt.encode(directory.size(), buffer);

            // Write directory entries
            for (DirectoryEntry entry : directory) {
                serializeDirectoryEntry(entry, buffer);
            }

            buffer.flip();
            return buffer.asReadOnlyBuffer();
        } catch (Exception e) {
            // Fallback to empty buffer if creation fails
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
        }
    }

    /**
     * Serialize this record to a ByteBuffer.
     * The returned buffer will be array-backed.
     */
    public ByteBuffer serializeToBuffer() {
        // Ensure directory is parsed for serialization
        ensureDirectoryParsed();

        var buffer = ByteBuffer.allocate(estimateSerializedSize());
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write header
        serializeHeader(buffer);

        // Write directory (always present)
        VarInt.encode(parsedDirectory.size(), buffer);
        for (var entry : parsedDirectory) {
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
     */
    public static ImprintRecordBuilder builder(SchemaId schemaId) {
        return new ImprintRecordBuilder(schemaId);
    }

    /**
     * Create a fluent builder for constructing ImprintRecord instances.
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
     * Deserialize a record from a ByteBuffer with lazy directory parsing.
     *
     * @param buffer the buffer to deserialize from. Must be array-backed
     *               (buffer.hasArray() == true) for optimal zero-copy performance.
     */
    public static ImprintRecord deserialize(ByteBuffer buffer) throws ImprintException {
        buffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // Read header
        var header = deserializeHeader(buffer);

        // Read directory count but don't parse entries yet
        int directoryStartPos = buffer.position();
        VarInt.DecodeResult countResult = VarInt.decode(buffer);
        int directoryCount = countResult.getValue();

        // Calculate directory buffer (includes count + all entries)
        int directorySize = countResult.getBytesRead() + (directoryCount * Constants.DIR_ENTRY_BYTES);
        buffer.position(directoryStartPos); // Reset to include count in directory buffer

        var directoryBuffer = buffer.slice();
        directoryBuffer.limit(directorySize);

        // Advance buffer past directory
        buffer.position(buffer.position() + directorySize);

        // Read payload as ByteBuffer slice for zero-copy
        var payload = buffer.slice();
        payload.limit(header.getPayloadSize());

        return new ImprintRecord(header, directoryBuffer, payload);
    }

    /**
     * Binary search for field ID in parsed directory.
     * Returns the index of the field if found, or a negative value if not found.
     */
    private int findDirectoryIndexInParsed(int fieldId) {
        if (!directoryParsed) {
            return -1;
        }

        int low = 0;
        int high = parsedDirectory.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midFieldId = parsedDirectory.get(mid).getId();

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
        size += VarInt.encodedLength(getDirectoryCount()); // directory count
        size += getDirectoryCount() * Constants.DIR_ENTRY_BYTES; // directory entries
        size += payload.remaining(); // payload
        return size;
    }

    private int getDirectoryCount() {
        if (directoryCount >= 0) {
            return directoryCount;
        }
        if (directoryParsed) {
            return parsedDirectory.size();
        }
        // Last resort: decode from buffer
        try {
            ByteBuffer countBuffer = directoryBuffer.duplicate();
            return VarInt.decode(countBuffer).getValue();
        } catch (Exception e) {
            return 0;
        }
    }

    // ===== EXISTING HELPER METHODS (unchanged) =====

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
        var valueSpecificBuffer = buffer.duplicate();
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

    // ===== TYPE-SPECIFIC GETTERS (unchanged API, improved performance) =====

    private <T extends Value> T getTypedValueOrThrow(int fieldId, TypeCode expectedTypeCode, Class<T> expectedValueClass, String expectedTypeName) throws ImprintException {
        var value = getValue(fieldId);

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
        var value = getValue(fieldId);

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
            return ((Value.BytesValue) value).getValue();
        }
        if (value instanceof Value.BytesBufferValue) {
            return ((Value.BytesBufferValue) value).getValue();
        }

        throw new ImprintException(ErrorType.TYPE_MISMATCH,
                "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected BYTES.");
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

    @Override
    public String toString() {
        return String.format("ImprintRecord{header=%s, directorySize=%d, payloadSize=%d, directoryParsed=%s}",
                header, getDirectoryCount(), payload.remaining(), directoryParsed);
    }
}