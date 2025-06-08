package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.TypeCode;
import com.imprint.util.VarInt;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Manages the raw buffers for an Imprint record with lazy directory parsing.
 * Encapsulates all buffer operations and provides zero-copy field access.
 *
 * <p><strong>Buffer Layout Overview:</strong></p>
 * <pre>
 * directoryBuffer: [VarInt count][DirectoryEntry 1][DirectoryEntry 2]...[DirectoryEntry N]
 * payload:         [Field 1 data][Field 2 data]...[Field N data]
 * </pre>
 *
 * <p>Each DirectoryEntry contains: [fieldId:2bytes][typeCode:1byte][offset:4bytes]</p>
 */
@Getter
public final class ImprintBuffers {
    private final ByteBuffer directoryBuffer; // Raw directory bytes (includes count)
    private final ByteBuffer payload; // Read-only payload view

    // Lazy-loaded directory state. Needs to maintain ordering so that we can binary search the endOffset
    private TreeMap<Integer, DirectoryEntry> parsedDirectory;
    private boolean directoryParsed = false;

    /**
     * Creates buffers from raw data (used during deserialization).
     */
    public ImprintBuffers(ByteBuffer directoryBuffer, ByteBuffer payload) {
        this.directoryBuffer = directoryBuffer.asReadOnlyBuffer();
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Creates buffers from a pre-parsed directory (used during construction).
     * This constructor is used by the ImprintRecordBuilder path. It creates
     * a serialized directory buffer but defers parsing it into a map until it's actually needed.
     */
    public ImprintBuffers(Collection<? extends DirectoryEntry> directory, ByteBuffer payload) {
        this.directoryBuffer = ImprintBuffers.createDirectoryBuffer(Objects.requireNonNull(directory));
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Creates buffers from a pre-parsed and sorted directory map (used by ImprintRecordBuilder).
     * This is an optimized path that avoids creating an intermediate List-to-Map conversion.
     * This constructor is used by the ImprintRecordBuilder path. It creates
     * a serialized directory buffer but defers parsing it into a map until it's actually needed.
     */
    public ImprintBuffers(TreeMap<Integer, ? extends DirectoryEntry> directoryMap, ByteBuffer payload) {
        this.directoryBuffer = ImprintBuffers.createDirectoryBufferFromMap(Objects.requireNonNull(directoryMap));
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Get a zero-copy ByteBuffer view of a field's data.
     * Optimized for the most common use case - single field access.
     */
    public ByteBuffer getFieldBuffer(int fieldId) throws ImprintException {
        var entry = findDirectoryEntry(fieldId);
        if (entry == null)
            return null;

        int startOffset = entry.getOffset();
        int endOffset = findEndOffset(entry);

        if (startOffset < 0 || endOffset < 0 || startOffset > payload.limit() ||
                endOffset > payload.limit() || startOffset > endOffset) {
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                    "Invalid field buffer range: start=" + startOffset + ", end=" + endOffset + ", payloadLimit=" + payload.limit());
        }

        var fieldBuffer = payload.duplicate();
        fieldBuffer.position(startOffset).limit(endOffset);
        return fieldBuffer;
    }

    /**
     * Find a directory entry for the given field ID using the most efficient method.
     * <p>
     * Strategy:
     * - If parsed: TreeMap lookup
     * - If raw: Binary search on raw bytes to avoid full unwinding of the directory
     */
    public DirectoryEntry findDirectoryEntry(int fieldId) throws ImprintException {
        if (directoryParsed)
            return parsedDirectory.get(fieldId);
         else
            return findFieldEntryInRawDirectory(fieldId);
    }

    /**
     * Get the full directory, parsing it if necessary.
     * Returns the values in fieldId order thanks to TreeMap.
     */
    public List<DirectoryEntry> getDirectory() {
        ensureDirectoryParsed();
        return new ArrayList<>(parsedDirectory.values());
    }

    /**
     * Get directory count without parsing.
     */
    public int getDirectoryCount() {
        if (directoryParsed)
            return parsedDirectory.size();
        try {
            var countBuffer = directoryBuffer.duplicate();
            return VarInt.decode(countBuffer).getValue();
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Create a new buffer containing the serialized directory.
     */
    public ByteBuffer serializeDirectory() {
        // The directoryBuffer is created on construction and is read-only.
        // If constructed from raw bytes, it's a view of the original.
        // If constructed from a list, it's a fresh buffer. In both cases, it's ready.
        return directoryBuffer.duplicate();
    }

    // ========== PRIVATE METHODS ==========

    /**
     * Binary search on raw directory bytes to find a specific field.
     * This avoids parsing the entire directory for single field lookups.
     */
    private DirectoryEntry findFieldEntryInRawDirectory(int fieldId) throws ImprintException {
        var searchBuffer = directoryBuffer.duplicate();
        searchBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int directoryCount = VarInt.decode(searchBuffer).getValue();
        if (directoryCount == 0)
            return null;

        int directoryStartPos = searchBuffer.position();
        int low = 0;
        int high = directoryCount - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryPos = directoryStartPos + (mid * Constants.DIR_ENTRY_BYTES);

            if (entryPos + Constants.DIR_ENTRY_BYTES > searchBuffer.limit()) {
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW,
                        "Directory entry at position " + entryPos + " exceeds buffer limit");
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

        return null;
    }

    /**
     * Find the end offset for a field by looking at the next field's offset.
     */
    private int findEndOffset(DirectoryEntry entry) throws ImprintException {
        if (directoryParsed) {
            return findNextOffsetInParsedDirectory(entry.getId());
        } else {
            return findNextOffsetInRawDirectory(entry.getId());
        }
    }

    /**
     * Find the end offset using TreeMap's efficient navigation methods.
     */
    private int findNextOffsetInParsedDirectory(int currentFieldId) {
        var nextEntry = parsedDirectory.higherEntry(currentFieldId);
        return nextEntry != null ? nextEntry.getValue().getOffset() : payload.limit();
    }

    private int findNextOffsetInRawDirectory(int currentFieldId) throws ImprintException {
        var scanBuffer = directoryBuffer.duplicate();
        scanBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int count = VarInt.decode(scanBuffer).getValue();
        if (count == 0)
            return payload.limit();

        int directoryStartPos = scanBuffer.position();
        int low = 0;
        int high = count - 1;
        int nextOffset = payload.limit();

        // Binary search for the first field with fieldId > currentFieldId
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryPos = directoryStartPos + (mid * Constants.DIR_ENTRY_BYTES);

            if (entryPos + Constants.DIR_ENTRY_BYTES > scanBuffer.limit())
                break;

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

    /**
     * Parse the full directory if not already parsed.
     * Creates TreeMap for both fast lookup and ordering needed for binary search on offSets.
     */
    private void ensureDirectoryParsed() {
        if (directoryParsed)
            return;
        try {
            var parseBuffer = directoryBuffer.duplicate();
            parseBuffer.order(ByteOrder.LITTLE_ENDIAN);

            var countResult = VarInt.decode(parseBuffer);
            int count = countResult.getValue();

            this.parsedDirectory = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                var entry = deserializeDirectoryEntry(parseBuffer);
                parsedDirectory.put((int)entry.getId(), entry);
            }

            this.directoryParsed = true;
        } catch (ImprintException e) {
            throw new RuntimeException("Failed to parse directory", e);
        }
    }

    /**
     * Create directory buffer from parsed entries.
     */
    static ByteBuffer createDirectoryBuffer(Collection<? extends DirectoryEntry> directory) {
        try {
            int bufferSize = VarInt.encodedLength(directory.size()) + (directory.size() * Constants.DIR_ENTRY_BYTES);
            var buffer = ByteBuffer.allocate(bufferSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            VarInt.encode(directory.size(), buffer);
            for (var entry : directory)
                serializeDirectoryEntry(entry, buffer);

            buffer.flip();
            return buffer.asReadOnlyBuffer();
        } catch (Exception e) {
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
        }
    }

    /**
     * Create directory buffer from a pre-sorted map of entries.
     */
    static ByteBuffer createDirectoryBufferFromMap(TreeMap<Integer, ? extends DirectoryEntry> directoryMap) {
        try {
            int bufferSize = VarInt.encodedLength(directoryMap.size()) + (directoryMap.size() * Constants.DIR_ENTRY_BYTES);
            var buffer = ByteBuffer.allocate(bufferSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            VarInt.encode(directoryMap.size(), buffer);
            for (var entry : directoryMap.values())
                serializeDirectoryEntry(entry, buffer);

            buffer.flip();
            return buffer.asReadOnlyBuffer();
        } catch (Exception e) {
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
        }
    }

    /**
     * Serialize a single directory entry to the buffer.
     * Format: [fieldId:2bytes][typeCode:1byte][offset:4bytes]
     */
    private static void serializeDirectoryEntry(DirectoryEntry entry, ByteBuffer buffer) {
        buffer.putShort(entry.getId());
        buffer.put(entry.getTypeCode().getCode());
        buffer.putInt(entry.getOffset());
    }

    /**
     * Deserialize a single directory entry from the buffer.
     * Reads: [fieldId:2bytes][typeCode:1byte][offset:4bytes]
     */
    private DirectoryEntry deserializeDirectoryEntry(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.DIR_ENTRY_BYTES)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for directory entry");

        short id = buffer.getShort();
        var typeCode = TypeCode.fromByte(buffer.get());
        int offset = buffer.getInt();

        return new SimpleDirectoryEntry(id, typeCode, offset);
    }
}