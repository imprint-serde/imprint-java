package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.ops.ImprintOperations;
import com.imprint.types.TypeCode;
import com.imprint.types.Value;
import com.imprint.util.VarInt;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.NonFinal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Imprint Record
 * <p>
 * This is the primary way to work with Imprint records, providing:
 * - Zero-copy field access via binary search
 * - Direct bytes-to-bytes operations (merge, project)
 * - Lazy deserializing operations
 */
@lombok.Value
@EqualsAndHashCode(of = "serializedBytes")
@ToString(of = {"header"})
public class ImprintRecord {
    ByteBuffer serializedBytes;

    @Getter(AccessLevel.PUBLIC)
    Header header;

    @Getter(AccessLevel.PACKAGE)
    // Raw directory bytes (read-only)
    ByteBuffer directoryBuffer;

    @Getter(AccessLevel.PACKAGE)
    // Raw payload bytes
    ByteBuffer payload;

    @NonFinal
    @Getter(AccessLevel.NONE)
    //Directory View cache to allow for easier mutable operations needed for lazy initialization
    Directory.DirectoryView directoryView;

    /**
     * Package-private constructor for @Value that creates immutable ByteBuffer views.
     */
    ImprintRecord(ByteBuffer serializedBytes, Header header, ByteBuffer directoryBuffer, ByteBuffer payload) {
        this.serializedBytes = serializedBytes.asReadOnlyBuffer();
        this.header = Objects.requireNonNull(header);
        this.directoryBuffer = directoryBuffer.asReadOnlyBuffer();
        this.payload = payload.asReadOnlyBuffer();
        this.directoryView = null;
    }

    // ========== STATIC FACTORY METHODS ==========

    /**
     * Create a builder for constructing new ImprintRecord instances.
     */
    public static ImprintRecordBuilder builder(SchemaId schemaId) {
        return new ImprintRecordBuilder(schemaId);
    }

    public static ImprintRecordBuilder builder(int fieldspaceId, int schemaHash) {
        return new ImprintRecordBuilder(new SchemaId(fieldspaceId, schemaHash));
    }

    /**
     * Deserialize an ImprintRecord from bytes.
     */
    public static ImprintRecord deserialize(byte[] bytes) throws ImprintException {
        return fromBytes(ByteBuffer.wrap(bytes));
    }

    public static ImprintRecord deserialize(ByteBuffer buffer) throws ImprintException {
        return fromBytes(buffer);
    }

    /**
     * Create a ImprintRecord from complete serialized bytes.
     */
    public static ImprintRecord fromBytes(ByteBuffer serializedBytes) throws ImprintException {
        Objects.requireNonNull(serializedBytes, "Serialized bytes cannot be null");

        var buffer = serializedBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // Parse header
        var header = parseHeader(buffer);

        // Extract directory and payload sections
        var parsedBuffers = parseBuffersFromSerialized(serializedBytes);

        return new ImprintRecord(serializedBytes, header, parsedBuffers.directoryBuffer, parsedBuffers.payload);
    }


    // ========== ZERO-COPY OPERATIONS ==========

    /**
     * Merge with another ImprintRecord using pure byte operations.
     * Results in a new ImprintRecord without any object creation.
     */
    public ImprintRecord merge(ImprintRecord other) throws ImprintException {
        var mergedBytes = ImprintOperations.mergeBytes(this.serializedBytes, other.serializedBytes);
        return fromBytes(mergedBytes);
    }

    /**
     * Project fields using pure byte operations.
     * Results in a new ImprintRecord without any object creation.
     */
    public ImprintRecord project(int... fieldIds) throws ImprintException {
        var projectedBytes = ImprintOperations.projectBytes(this.serializedBytes, fieldIds);
        return fromBytes(projectedBytes);
    }

    /**
     * Chain multiple operations efficiently.
     * Each operation works on bytes without creating intermediate objects.
     */
    public ImprintRecord projectAndMerge(ImprintRecord other, int... projectFields) throws ImprintException {
        return this.project(projectFields).merge(other);
    }

    /**
     * Get the raw serialized bytes.
     * This is the most efficient way to pass the record around.
     */
    public ByteBuffer getSerializedBytes() {
        return serializedBytes.duplicate();
    }

    /**
     * Get a DirectoryView for straight through directory access.
     */
    public Directory.DirectoryView getDirectoryView() {
        if (directoryView == null) {
            directoryView = new ImprintDirectoryView();
        }
        return directoryView;
    }

    /**
     * Get the directory list.
     */
    public List<Directory> getDirectory() {
        return getDirectoryView().toList();
    }

    /**
     * Get raw bytes for a field without deserializing.
     */
    public ByteBuffer getRawBytes(int fieldId) {
        try {
            return getFieldBuffer(fieldId);
        } catch (ImprintException e) {
            return null;
        }
    }

    /**
     * Get raw bytes for a field by short ID.
     */
    public ByteBuffer getRawBytes(short fieldId) {
        return getRawBytes((int) fieldId);
    }

    /**
     * Get a field value by ID.
     * Uses zero-copy binary search to locate the field.
     */
    public Value getValue(int fieldId) throws ImprintException {
        var entry = getDirectoryView().findEntry(fieldId);
        if (entry == null) return null;

        var fieldBuffer = getFieldBuffer(fieldId);
        if (fieldBuffer == null) return null;

        return deserializeValue(entry.getTypeCode(), fieldBuffer);
    }

    /**
     * Check if a field exists without deserializing it.
     */
    public boolean hasField(int fieldId) {
        return getDirectoryView().findEntry(fieldId) != null;
    }

    /**
     * Get the number of fields without parsing the directory.
     */
    public int getFieldCount() {
        return getDirectoryCount();
    }

    // ========== TYPED GETTERS ==========

    public String getString(int fieldId) throws ImprintException {
        var value = getValidatedValue(fieldId, "STRING");
        if (value instanceof Value.StringValue)
            return ((Value.StringValue) value).getValue();
        if (value instanceof Value.StringBufferValue)
            return ((Value.StringBufferValue) value).getValue();
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is not a STRING");
    }

    public int getInt32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.INT32, Value.Int32Value.class, "int32").getValue();
    }

    public long getInt64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.INT64, Value.Int64Value.class, "int64").getValue();
    }

    public boolean getBoolean(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.BOOL, Value.BoolValue.class, "boolean").getValue();
    }

    public float getFloat32(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.FLOAT32, Value.Float32Value.class, "float32").getValue();
    }

    public double getFloat64(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.FLOAT64, Value.Float64Value.class, "float64").getValue();
    }

    public byte[] getBytes(int fieldId) throws ImprintException {
        var value = getValidatedValue(fieldId, "BYTES");
        if (value instanceof Value.BytesValue)
            return ((Value.BytesValue) value).getValue();
        if (value instanceof Value.BytesBufferValue)
            return ((Value.BytesBufferValue) value).getValue();
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is not BYTES");
    }

    public java.util.List<Value> getArray(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.ARRAY, Value.ArrayValue.class, "ARRAY").getValue();
    }

    public java.util.Map<com.imprint.types.MapKey, Value> getMap(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.MAP, Value.MapValue.class, "MAP").getValue();
    }

    public ImprintRecord getRow(int fieldId) throws ImprintException {
        return getTypedValueOrThrow(fieldId, com.imprint.types.TypeCode.ROW, Value.RowValue.class, "ROW").getValue();
    }

    /**
     * Returns a copy of the bytes.
     */
    public ByteBuffer serializeToBuffer() {
        return serializedBytes.duplicate();
    }

    /**
     * Get the schema ID from the header.
     */
    public SchemaId getSchemaId() {
        return header.getSchemaId();
    }

    /**
     * Estimate the memory footprint of this record.
     */
    public int getSerializedSize() {
        return serializedBytes.remaining();
    }


    /**
     * Get and validate a value exists and is not null.
     */
    private Value getValidatedValue(int fieldId, String typeName) throws ImprintException {
        var value = getValue(fieldId);
        if (value == null)
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND, "Field " + fieldId + " not found");
        if (value.getTypeCode() == com.imprint.types.TypeCode.NULL)
            throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is NULL, cannot retrieve as " + typeName);
        return value;
    }

    private <T extends Value> T getTypedValueOrThrow(int fieldId, com.imprint.types.TypeCode expectedTypeCode, Class<T> expectedValueClass, String expectedTypeName)
            throws ImprintException {
        var value = getValidatedValue(fieldId, expectedTypeName);
        if (value.getTypeCode() == expectedTypeCode && expectedValueClass.isInstance(value))
            return expectedValueClass.cast(value);
        throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is of type " + value.getTypeCode() + ", expected " + expectedTypeName);
    }

    /**
     * Parse buffers from serialized record bytes.
     */
    private static ParsedBuffers parseBuffersFromSerialized(ByteBuffer serializedRecord) throws ImprintException {
        var buffer = serializedRecord.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // Parse header and extract sections using shared utility
        var header = parseHeaderFromBuffer(buffer);
        var sections = extractBufferSections(buffer, header);

        return new ParsedBuffers(sections.directoryBuffer, sections.payloadBuffer);
    }

    private static class ParsedBuffers {
        final ByteBuffer directoryBuffer;
        final ByteBuffer payload;

        ParsedBuffers(ByteBuffer directoryBuffer, ByteBuffer payload) {
            this.directoryBuffer = directoryBuffer;
            this.payload = payload;
        }
    }

    private int getDirectoryCount() {
        try {
            return VarInt.decode(directoryBuffer.duplicate()).getValue();
        } catch (ImprintException e) {
            return  0; // Cache as 0 on error
        }
    }

    /**
     * Gets ByteBuffer view of a field's data.
     */
    private ByteBuffer getFieldBuffer(int fieldId) throws ImprintException {
        var entry = findDirectoryEntry(fieldId);
        if (entry == null)
            return null;

        int startOffset = entry.getOffset();
        int endOffset = findEndOffset(entry.getId());

        if (startOffset < 0 || endOffset < 0 || startOffset > payload.limit() ||
                endOffset > payload.limit() || startOffset > endOffset) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Invalid field buffer range: start=" + startOffset + ", end=" + endOffset);
        }

        var fieldBuffer = payload.duplicate();
        fieldBuffer.position(startOffset).limit(endOffset);
        return fieldBuffer;
    }

    private Directory findDirectoryEntry(int fieldId) throws ImprintException {
        var searchBuffer = directoryBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        int count = getDirectoryCount();
        if (count == 0) return null;

        // Advance past varint to entries
        VarInt.decode(searchBuffer);
        int directoryStartPos = searchBuffer.position();

        int low = 0;
        int high = count - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryPos = directoryStartPos + (mid * Constants.DIR_ENTRY_BYTES);

            if (entryPos + Constants.DIR_ENTRY_BYTES > searchBuffer.limit())
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Directory entry exceeds buffer");

            searchBuffer.position(entryPos);
            short midFieldId = searchBuffer.getShort();

            if (midFieldId < fieldId) {
                low = mid + 1;
            } else if (midFieldId > fieldId) {
                high = mid - 1;
            } else {
                // Found it - read complete entry
                searchBuffer.position(entryPos);
                return deserializeDirectoryEntry(searchBuffer);
            }
        }

        return null;
    }

    private int findEndOffset(int currentFieldId) throws ImprintException {
        var scanBuffer = directoryBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        int count = getDirectoryCount();
        if (count == 0) return payload.limit();

        // Advance past varint
        VarInt.decode(scanBuffer);
        int directoryStartPos = scanBuffer.position();

        int low = 0;
        int high = count - 1;
        int nextOffset = payload.limit();

        // Binary search for first field with fieldId > currentFieldId
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryPos = directoryStartPos + (mid * Constants.DIR_ENTRY_BYTES);

            if (entryPos + Constants.DIR_ENTRY_BYTES > scanBuffer.limit()) break;

            scanBuffer.position(entryPos);
            short fieldId = scanBuffer.getShort();
            scanBuffer.get(); // skip type
            int offset = scanBuffer.getInt();

            if (fieldId > currentFieldId) {
                nextOffset = offset;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return nextOffset;
    }

    private Directory deserializeDirectoryEntry(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.DIR_ENTRY_BYTES)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for directory entry");

        short id = buffer.getShort();
        var typeCode = TypeCode.fromByte(buffer.get());
        int offset = buffer.getInt();

        return new Directory.Entry(id, typeCode, offset);
    }

    /**
     * DirectoryView
     */
    private class ImprintDirectoryView implements Directory.DirectoryView {

        @Override
        public Directory findEntry(int fieldId) {
            try {
                return findDirectoryEntry(fieldId);
            } catch (ImprintException e) {
                return null;
            }
        }

        /**
         * List out all directories in the buffer. This operation unpacks any directories not already deserialized
         * so proceed only if eager evaluation is intended.
         */
        @Override
        public List<Directory> toList() {
            var list = new ArrayList<Directory>(getDirectoryCount());
            var iterator = iterator();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return list;
        }

        @Override
        public int size() {
            return getDirectoryCount();
        }

        @Override
        public Iterator<Directory> iterator() {
            return new ImprintDirectoryIterator();
        }
    }

    /**
     * Iterator that parses directory entries lazily from raw bytes.
     */
    private class ImprintDirectoryIterator implements Iterator<Directory> {
        private final ByteBuffer iterBuffer;
        private final int totalCount;
        private int currentIndex;

        ImprintDirectoryIterator() {
            this.iterBuffer = directoryBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            this.totalCount = getDirectoryCount();

            try {
                // Skip past varint to first entry
                VarInt.decode(iterBuffer);
            } catch (ImprintException e) {
                throw new RuntimeException("Failed to initialize directory iterator", e);
            }
            this.currentIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < totalCount;
        }

        @Override
        public Directory next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                var entry = deserializeDirectoryEntry(iterBuffer);
                currentIndex++;
                return entry;
            } catch (ImprintException e) {
                throw new RuntimeException("Failed to parse directory entry at index " + currentIndex, e);
            }
        }
    }

    static void writeDirectoryToBuffer(short[] sortedKeys, Object[] sortedValues, int[] offsets, int fieldCount, ByteBuffer buffer) {
        // Optimize VarInt encoding for common case (< 128 fields = single byte)
        if (fieldCount < 128) {
            buffer.put((byte) fieldCount);
        } else {
            VarInt.encode(fieldCount, buffer);
        }
        
        // Early return for empty directory
        if (fieldCount == 0) {
            return;
        }
        
        // Tight loop optimization: minimize method calls and casts
        for (int i = 0; i < fieldCount; i++) {
            var entry = (ImprintRecordBuilder.ValueWithType) sortedValues[i];
            
            // Get current position once, then batch write
            int pos = buffer.position();
            
            // Write all 7 bytes for this entry in sequence
            buffer.putShort(pos, sortedKeys[i]);           // bytes 0-1: field ID  
            buffer.put(pos + 2, entry.typeCode);    // byte 2: type code
            buffer.putInt(pos + 3, offsets[i]);     // bytes 3-6: offset
            
            // Advance buffer position by 7 bytes
            buffer.position(pos + 7);
        }
    }

    /**
     * Parse a header from a ByteBuffer without advancing the buffer position.
     * Utility method shared between {@link ImprintRecord} and {@link ImprintOperations}.
     */
    public static Header parseHeaderFromBuffer(ByteBuffer buffer) throws ImprintException {
        int startPos = buffer.position();
        try {
            return parseHeader(buffer);
        } finally {
            buffer.position(startPos);
        }
    }

    /**
     * Calculate the size needed to store a directory with the given entry count.
     */
    public static int calculateDirectorySize(int entryCount) {
        return VarInt.encodedLength(entryCount) + (entryCount * Constants.DIR_ENTRY_BYTES);
    }

    /**
     * Container for separated directory and payload buffer sections.
     * Utility class shared between {@link ImprintRecord} and {@link ImprintOperations}.
     */
    public static class BufferSections {
        public final ByteBuffer directoryBuffer;
        public final ByteBuffer payloadBuffer;
        public final int directoryCount;

        public BufferSections(ByteBuffer directoryBuffer, ByteBuffer payloadBuffer, int directoryCount) {
            this.directoryBuffer = directoryBuffer;
            this.payloadBuffer = payloadBuffer;
            this.directoryCount = directoryCount;
        }
    }

    /**
     * Extract directory and payload sections from a serialized buffer.
     * Utility method shared between {@link ImprintRecord} and {@link ImprintOperations}.
     */
    public static BufferSections extractBufferSections(ByteBuffer buffer, Header header) throws ImprintException {
        // Skip header
        buffer.position(buffer.position() + Constants.HEADER_BYTES);

        // Parse directory section
        int directoryStartPos = buffer.position();
        var countResult = VarInt.decode(buffer);
        int directoryCount = countResult.getValue();
        int directorySize = countResult.getBytesRead() + (directoryCount * Constants.DIR_ENTRY_BYTES);

        // Create directory buffer
        buffer.position(directoryStartPos);
        var directoryBuffer = buffer.slice();
        directoryBuffer.limit(directorySize);

        // Advance to payload
        buffer.position(buffer.position() + directorySize);
        var payloadBuffer = buffer.slice();
        payloadBuffer.limit(header.getPayloadSize());

        return new BufferSections(directoryBuffer, payloadBuffer, directoryCount);
    }

    private static Header parseHeader(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.HEADER_BYTES)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for header");

        byte magic = buffer.get();
        byte version = buffer.get();

        if (magic != Constants.MAGIC)
            throw new ImprintException(ErrorType.INVALID_MAGIC, "Invalid magic byte");
        if (version != Constants.VERSION)
            throw new ImprintException(ErrorType.UNSUPPORTED_VERSION, "Unsupported version: " + version);

        var flags = new Flags(buffer.get());
        int fieldSpaceId = buffer.getInt();
        int schemaHash = buffer.getInt();
        int payloadSize = buffer.getInt();

        return new Header(flags, new SchemaId(fieldSpaceId, schemaHash), payloadSize);
    }

    private Value deserializeValue(com.imprint.types.TypeCode typeCode, ByteBuffer buffer) throws ImprintException {
        var valueBuffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
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
}