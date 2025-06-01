
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
 * An Imprint record containing a header, optional field directory, and payload.
 * Uses ByteBuffer for zero-copy operations to achieve low latency.
 */
@Getter
public final class ImprintRecord {
    private final Header header;
    private final List<DirectoryEntry> directory;
    private final ByteBuffer payload; // Read-only view for zero-copy
    
    public ImprintRecord(Header header, List<DirectoryEntry> directory, ByteBuffer payload) {
        this.header = Objects.requireNonNull(header, "Header cannot be null");
        this.directory = List.copyOf(Objects.requireNonNull(directory, "Directory cannot be null"));
        this.payload = payload.asReadOnlyBuffer(); // Zero-copy read-only view
    }

    /**
     * Get a value by field ID, deserializing it on demand.
     */
    public Optional<Value> getValue(int fieldId) throws ImprintException {
        // Binary search for the field ID without allocation
        int index = findDirectoryIndex(fieldId);
        if (index < 0) return Optional.empty();
        
        var entry = directory.get(index);
        int startOffset = entry.getOffset();
        int endOffset = (index + 1 < directory.size()) ? 
            directory.get(index + 1).getOffset() : payload.remaining();
            
        var valueBytes = payload.duplicate();
        valueBytes.position(startOffset).limit(endOffset);
        var value = deserializeValue(entry.getTypeCode(), valueBytes.slice());
        return Optional.of(value);
    }
    
    /**
     * Get the raw bytes for a field without deserializing.
     * Returns a zero-copy ByteBuffer view.
     */
    public Optional<ByteBuffer> getRawBytes(int fieldId) {
        int index = findDirectoryIndex(fieldId);
        if (index < 0) return Optional.empty();

        var entry = directory.get(index);
        int startOffset = entry.getOffset();
        int endOffset = (index + 1 < directory.size()) ? 
            directory.get(index + 1).getOffset() : payload.remaining();
            
        var fieldBuffer = payload.duplicate();
        fieldBuffer.position(startOffset).limit(endOffset);
        return Optional.of(fieldBuffer.slice().asReadOnlyBuffer());
    }
    
    /**
     * Serialize this record to a ByteBuffer (zero-copy when possible).
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
        
        // Return read-only view of used portion
        buffer.flip();
        return buffer.asReadOnlyBuffer();
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
    public static ImprintRecordBuilder builder(int fieldspaceId, int schemaHash) {
        return new ImprintRecordBuilder(new SchemaId(fieldspaceId, schemaHash));
    }
    
    /**
     * Deserialize a record from bytes.
     */
    public static ImprintRecord deserialize(byte[] bytes) throws ImprintException {
        return deserialize(ByteBuffer.wrap(bytes));
    }
    
    /**
     * Deserialize a record from a ByteBuffer (zero-copy when possible).
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
    
    private int estimateSerializedSize() {
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
        buffer.putInt(header.getSchemaId().getFieldspaceId());
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
        // Buffer is already positioned and limited correctly
        buffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        
        // Use TypeHandler for simple types
        switch (typeCode) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
                return typeCode.getHandler().deserialize(buffer);

            case ARRAY:
                return deserializeArray(buffer);

            case MAP:
                return deserializeMap(buffer);

            case ROW:
                var remainingBuffer = buffer.slice();
                var nestedRecord = deserialize(remainingBuffer);
                return Value.fromRow(nestedRecord);

            default:
                throw new ImprintException(ErrorType.INVALID_TYPE_CODE, "Unknown type code: " + typeCode);
        }
    }
    
    private Value deserializeArray(ByteBuffer buffer) throws ImprintException {
        VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
        int length = lengthResult.getValue();
        
        if (length == 0) {
            return Value.fromArray(Collections.emptyList());
        }
        
        var elementType = TypeCode.fromByte(buffer.get());
        var elements = new ArrayList<Value>(length);
        
        for (int i = 0; i < length; i++) {
            var elementBytes = readValueBytes(elementType, buffer);
            var element = deserializeValue(elementType, elementBytes);
            elements.add(element);
        }
        
        return Value.fromArray(elements);
    }
    
    private Value deserializeMap(ByteBuffer buffer) throws ImprintException {
        VarInt.DecodeResult lengthResult = VarInt.decode(buffer);
        int length = lengthResult.getValue();
        
        if (length == 0) {
            return Value.fromMap(Collections.emptyMap());
        }
        
        var keyType = TypeCode.fromByte(buffer.get());
        var valueType = TypeCode.fromByte(buffer.get());
        var map = new HashMap<MapKey, Value>(length);
        
        for (int i = 0; i < length; i++) {
            // Read key
            var keyBytes = readValueBytes(keyType, buffer);
            var keyValue = deserializeValue(keyType, keyBytes);
            var key = MapKey.fromValue(keyValue);
            
            // Read value
            var valueBytes = readValueBytes(valueType, buffer);
            var value = deserializeValue(valueType, valueBytes);
            
            map.put(key, value);
        }
        
        return Value.fromMap(map);
    }
    
    private ByteBuffer readValueBytes(TypeCode typeCode, ByteBuffer buffer) throws ImprintException {
        // Use TypeHandler for simple types
        switch (typeCode) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
                return typeCode.getHandler().readValueBytes(buffer);

            case ARRAY:
            case MAP:
            case ROW:
                // For complex types, return the entire remaining buffer for now
                // The specific deserializer will handle parsing in the future
                var remainingBuffer = buffer.slice();
                buffer.position(buffer.limit());
                return remainingBuffer.asReadOnlyBuffer();

            default:
                throw new ImprintException(ErrorType.INVALID_TYPE_CODE, "Unknown type code: " + typeCode);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        var that = (ImprintRecord) obj;
        return header.equals(that.header) &&
               directory.equals(that.directory) &&
               payload.equals(that.payload);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(header, directory, payload);
    }
    
    @Override
    public String toString() {
        return String.format("ImprintRecord{header=%s, directorySize=%d, payloadSize=%d}", 
                           header, directory.size(), payload.remaining());
    }
}