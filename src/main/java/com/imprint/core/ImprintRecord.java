package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.ops.ImprintOperations;
import com.imprint.types.ImprintDeserializers;
import com.imprint.types.TypeCode;
import com.imprint.util.ImprintBuffer;
import com.imprint.util.VarInt;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.NonFinal;

import java.nio.ByteBuffer;
import java.util.*;

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
    ImprintBuffer serializedBytes;

    @Getter(AccessLevel.PUBLIC)
    Header header;

    @Getter(AccessLevel.PACKAGE)
    // Raw directory bytes (read-only)
    ImprintBuffer directoryBuffer;

    @Getter(AccessLevel.PACKAGE)
    // Raw payload bytes
    ImprintBuffer payload;

    @NonFinal
    @Getter(AccessLevel.NONE)
    //Directory View cache to allow for easier mutable operations needed for lazy initialization
    Directory.DirectoryView directoryView;

    /**
     * Package-private constructor for @Value that creates immutable ByteBuffer views.
     */
    ImprintRecord(ImprintBuffer serializedBytes, Header header, ImprintBuffer directoryBuffer, ImprintBuffer payload) {
        // Debug: Log buffer details for empty records during construction
        if (serializedBytes.remaining() <= 16) {  // Log for small buffers
            System.err.println("DEBUG: ImprintRecord constructor - buffer details:");
            System.err.println("  input serializedBytes.remaining()=" + serializedBytes.remaining());
            System.err.println("  input serializedBytes.position()=" + serializedBytes.position());
            System.err.println("  input serializedBytes.limit()=" + serializedBytes.limit());
        }
        
        this.serializedBytes = serializedBytes.asReadOnlyBuffer();
        
        // Debug: Log buffer details after asReadOnlyBuffer()
        if (this.serializedBytes.remaining() <= 16) {  // Log for small buffers
            System.err.println("  after asReadOnlyBuffer().remaining()=" + this.serializedBytes.remaining());
            System.err.println("  after asReadOnlyBuffer().position()=" + this.serializedBytes.position());
            System.err.println("  after asReadOnlyBuffer().limit()=" + this.serializedBytes.limit());
        }
        
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

    /**
     * Create a pre-sized builder for constructing new ImprintRecord instances.
     * @param schemaId Schema identifier
     * @param expectedFieldCount Expected number of fields to optimize memory allocation
     */
    public static ImprintRecordBuilder builder(SchemaId schemaId, int expectedFieldCount) {
        return new ImprintRecordBuilder(schemaId, expectedFieldCount);
    }

    public static ImprintRecordBuilder builder(int fieldspaceId, int schemaHash) {
        return new ImprintRecordBuilder(new SchemaId(fieldspaceId, schemaHash));
    }

    /**
     * Deserialize an ImprintRecord from bytes.
     */
    public static ImprintRecord deserialize(byte[] bytes) throws ImprintException {
        return fromBytes(new ImprintBuffer(bytes));
    }

    public static ImprintRecord deserialize(ImprintBuffer buffer) throws ImprintException {
        return fromBytes(buffer);
    }

    /**
     * Create a ImprintRecord from complete serialized bytes.
     */
    public static ImprintRecord fromBytes(ImprintBuffer serializedBytes) throws ImprintException {
        Objects.requireNonNull(serializedBytes, "Serialized bytes cannot be null");

        // Debug: Log buffer details for empty records in fromBytes
        if (serializedBytes.remaining() <= 16) {  // Log for small buffers
            System.err.println("DEBUG: ImprintRecord.fromBytes() - input buffer details:");
            System.err.println("  input serializedBytes.remaining()=" + serializedBytes.remaining());
            System.err.println("  input serializedBytes.position()=" + serializedBytes.position());
            System.err.println("  input serializedBytes.limit()=" + serializedBytes.limit());
        }

        var buffer = serializedBytes.duplicate();

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
        var mergedBytes = ImprintOperations.mergeBytes(this.getSerializedBytes(), other.getSerializedBytes());
        return fromBytes(mergedBytes);
    }

    /**
     * Project fields using pure byte operations.
     * Results in a new ImprintRecord without any object creation.
     */
    public ImprintRecord project(int... fieldIds) throws ImprintException {
        var projectedBytes = ImprintOperations.projectBytes(this.getSerializedBytes(), fieldIds);
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
    public ImprintBuffer getSerializedBytes() {
        var buffer = serializedBytes.duplicate();
        buffer.position(0);
        return buffer;
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
    public ImprintBuffer getRawBytes(int fieldId) {
        try {
            return getFieldBuffer(fieldId);
        } catch (ImprintException e) {
            return null;
        }
    }

    /**
     * Get raw bytes for a field by short ID.
     */
    public ImprintBuffer getRawBytes(short fieldId) {
        return getRawBytes((int) fieldId);
    }

    /**
     * Get a field value by ID as Object.
     * Uses zero-copy binary search to locate the field.
     */
    public Object getValue(int fieldId) throws ImprintException {
        var entry = getDirectoryView().findEntry(fieldId);
        if (entry == null) return null;

        var fieldBuffer = getFieldBuffer(fieldId);
        if (fieldBuffer == null) return null;

        return deserializePrimitive(entry.getTypeCode(), fieldBuffer);
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
        return (String) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.STRING, "STRING");
    }

    public int getInt32(int fieldId) throws ImprintException {
        return (Integer) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.INT32, "INT32");
    }

    public long getInt64(int fieldId) throws ImprintException {
        return (Long) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.INT64, "INT64");
    }

    public boolean getBoolean(int fieldId) throws ImprintException {
        return (Boolean) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.BOOL, "BOOL");
    }

    public float getFloat32(int fieldId) throws ImprintException {
        return (Float) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.FLOAT32, "FLOAT32");
    }

    public double getFloat64(int fieldId) throws ImprintException {
        return (Double) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.FLOAT64, "FLOAT64");
    }

    public byte[] getBytes(int fieldId) throws ImprintException {
        return (byte[]) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.BYTES, "BYTES");
    }

    @SuppressWarnings("unchecked")
    public <T> java.util.List<T> getArray(int fieldId) throws ImprintException {
        return (java.util.List<T>) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.ARRAY, "ARRAY");
    }

    @SuppressWarnings("unchecked")
    public <K, V> java.util.Map<K, V> getMap(int fieldId) throws ImprintException {
        return (java.util.Map<K, V>) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.MAP, "MAP");
    }

    public ImprintRecord getRow(int fieldId) throws ImprintException {
        return (ImprintRecord) getTypedPrimitive(fieldId, com.imprint.types.TypeCode.ROW, "ROW");
    }

    /**
     * Returns a copy of the bytes.
     */
    public ImprintBuffer serializeToBuffer() {
        // Debug: Log original buffer state
        if (serializedBytes.remaining() <= 20) {  // Log for small buffers
            System.err.println("DEBUG: serializeToBuffer() - original serializedBytes:");
            System.err.println("  serializedBytes.remaining()=" + serializedBytes.remaining());
            System.err.println("  serializedBytes.position()=" + serializedBytes.position());
            System.err.println("  serializedBytes.limit()=" + serializedBytes.limit());
        }
        
        var buffer = serializedBytes.duplicate();
        
        // Debug: Log after duplicate
        if (buffer.remaining() <= 20) {  // Log for small buffers
            System.err.println("  after duplicate().remaining()=" + buffer.remaining());
            System.err.println("  after duplicate().position()=" + buffer.position());
            System.err.println("  after duplicate().limit()=" + buffer.limit());
        }
        
        buffer.position(0);
        
        // Debug: Log after position(0)
        if (buffer.remaining() <= 20) {  // Log for small buffers
            System.err.println("  after position(0).remaining()=" + buffer.remaining());
            System.err.println("  after position(0).position()=" + buffer.position());
            System.err.println("  after position(0).limit()=" + buffer.limit());
        }
        
        // Debug: Log buffer size for empty records  
        if (buffer.remaining() < Constants.HEADER_BYTES) {
            System.err.println("WARNING: serializeToBuffer() returning undersized buffer: " + 
                              buffer.remaining() + " bytes (minimum " + Constants.HEADER_BYTES + " required)");
            System.err.println("Buffer details: position=" + buffer.position() + ", limit=" + buffer.limit() + ", capacity=" + buffer.capacity());
        }
        
        return buffer;
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
     * Get and validate a field exists, is not null, and has the expected type.
     */
    private Object getTypedPrimitive(int fieldId, TypeCode expectedTypeCode, String typeName) throws ImprintException {
        var entry = getDirectoryView().findEntry(fieldId);
        if (entry == null)
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND, "Field " + fieldId + " not found");

        if (entry.getTypeCode() == TypeCode.NULL)
            throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is NULL, cannot retrieve as " + typeName);

        if (entry.getTypeCode() != expectedTypeCode)
            throw new ImprintException(ErrorType.TYPE_MISMATCH, "Field " + fieldId + " is of type " + entry.getTypeCode() + ", expected " + typeName);

        var fieldBuffer = getFieldBuffer(fieldId);
        if (fieldBuffer == null)
            throw new ImprintException(ErrorType.FIELD_NOT_FOUND, "Field " + fieldId + " buffer not found");

        return deserializePrimitive(entry.getTypeCode(), fieldBuffer);
    }

    /**
     * Parse buffers from serialized record bytes.
     */
    private static ParsedBuffers parseBuffersFromSerialized(ImprintBuffer serializedRecord) throws ImprintException {
        var buffer = serializedRecord.duplicate();

        // Parse header and extract sections using shared utility
        var header = parseHeaderFromBuffer(buffer);
        var sections = extractBufferSections(buffer, header);

        return new ParsedBuffers(sections.directoryBuffer, sections.payloadBuffer);
    }

    private static class ParsedBuffers {
        final ImprintBuffer directoryBuffer;
        final ImprintBuffer payload;

        ParsedBuffers(ImprintBuffer directoryBuffer, ImprintBuffer payload) {
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
    private ImprintBuffer getFieldBuffer(int fieldId) throws ImprintException {
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
        var searchBuffer = directoryBuffer.duplicate();

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
        var scanBuffer = directoryBuffer.duplicate();

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

    private Directory deserializeDirectoryEntry(ImprintBuffer buffer) throws ImprintException {
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
        private final ImprintBuffer iterBuffer;
        private final int totalCount;
        private int currentIndex;

        ImprintDirectoryIterator() {
            this.iterBuffer = directoryBuffer.duplicate();
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

    /**
     * Parse a header from a ByteBuffer without advancing the buffer position.
     * Utility method shared between {@link ImprintRecord} and {@link ImprintOperations}.
     */
    public static Header parseHeaderFromBuffer(ImprintBuffer buffer) throws ImprintException {
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
        public final ImprintBuffer directoryBuffer;
        public final ImprintBuffer payloadBuffer;
        public final int directoryCount;

        public BufferSections(ImprintBuffer directoryBuffer, ImprintBuffer payloadBuffer, int directoryCount) {
            this.directoryBuffer = directoryBuffer;
            this.payloadBuffer = payloadBuffer;
            this.directoryCount = directoryCount;
        }
    }

    /**
     * Extract directory and payload sections from a serialized buffer.
     * Utility method shared between {@link ImprintRecord} and {@link ImprintOperations}.
     */
    public static BufferSections extractBufferSections(ImprintBuffer buffer, Header header) throws ImprintException {
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

    private static Header parseHeader(ImprintBuffer buffer) throws ImprintException {
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

    private Object deserializePrimitive(com.imprint.types.TypeCode typeCode, ImprintBuffer buffer) throws ImprintException {
        var valueBuffer = buffer.duplicate();
        switch (typeCode) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
                return ImprintDeserializers.deserializePrimitive(valueBuffer, typeCode);
            case ARRAY:
                return deserializePrimitiveArray(valueBuffer);
            case MAP:
                return deserializePrimitiveMap(valueBuffer);
            case ROW:
                return deserialize(valueBuffer);
            default:
                throw new ImprintException(ErrorType.INVALID_TYPE_CODE, "Unknown type code: " + typeCode);
        }
    }

    private List<Object> deserializePrimitiveArray(ImprintBuffer buffer) throws ImprintException {
        VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
        int length = lengthResult.getValue();

        if (length == 0) {
            return Collections.emptyList();
        }

        if (buffer.remaining() < 1) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for ARRAY element type code.");
        }
        var elementType = TypeCode.fromByte(buffer.get());
        var elements = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            Object element;
            if (elementType == TypeCode.ARRAY) {
                element = deserializePrimitiveArray(buffer);
            } else if (elementType == TypeCode.MAP) {
                element = deserializePrimitiveMap(buffer);
            } else if (elementType == TypeCode.ROW) {
                element = deserialize(buffer);
            } else {
                element = ImprintDeserializers.deserializePrimitive(buffer, elementType);
            }
            elements.add(element);
        }

        return elements;
    }

    private Map<Object, Object> deserializePrimitiveMap(ImprintBuffer buffer) throws ImprintException {
        VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
        int length = lengthResult.getValue();

        if (length == 0) {
            return Collections.emptyMap();
        }

        if (buffer.remaining() < 2) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for MAP key/value type codes.");
        }
        var keyType = TypeCode.fromByte(buffer.get());
        var valueType = TypeCode.fromByte(buffer.get());
        var map = new HashMap<>(length);

        for (int i = 0; i < length; i++) {
            var keyPrimitive = ImprintDeserializers.deserializePrimitive(buffer, keyType);
            
            Object valuePrimitive;
            if (valueType == TypeCode.ARRAY) {
                valuePrimitive = deserializePrimitiveArray(buffer);
            } else if (valueType == TypeCode.MAP) {
                valuePrimitive = deserializePrimitiveMap(buffer);
            } else if (valueType == TypeCode.ROW) {
                valuePrimitive = deserialize(buffer);
            } else {
                valuePrimitive = ImprintDeserializers.deserializePrimitive(buffer, valueType);
            }
            map.put(keyPrimitive, valuePrimitive);
        }

        return map;
    }
}