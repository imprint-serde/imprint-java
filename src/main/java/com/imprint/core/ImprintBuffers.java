package com.imprint.core;

import com.imprint.Constants;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.TypeCode;
import com.imprint.util.VarInt;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
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

    // Lazy-loaded directory state.
    private Int2ObjectSortedMap<DirectoryEntry> parsedDirectory;
    private boolean directoryParsed = false;
    private int directoryCount = -1;

    /**
     * Creates buffers from raw data (used during deserialization).
     */
    public ImprintBuffers(ByteBuffer directoryBuffer, ByteBuffer payload) {
        this.directoryBuffer = directoryBuffer.asReadOnlyBuffer();
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Creates buffers from a pre-sorted list of entries (most efficient builder path).
     * Immediately creates the parsed index and the serialized buffer.
     */
    public ImprintBuffers(List<? extends DirectoryEntry> sortedDirectory, ByteBuffer payload) {
        this.directoryBuffer = ImprintBuffers.createDirectoryBuffer(sortedDirectory);
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Creates buffers from a pre-parsed and sorted directory map containing final, simple entries.
     * This is the most efficient path, as it avoids any further parsing or sorting. The provided
     * map becomes the definitive parsed directory.
     */
    @SuppressWarnings("unchecked")
    public ImprintBuffers(Int2ObjectSortedMap<? extends DirectoryEntry> parsedDirectory, ByteBuffer payload) {
        this.directoryBuffer = ImprintBuffers.createDirectoryBufferFromSortedMap(Objects.requireNonNull(parsedDirectory));
        this.payload = payload.asReadOnlyBuffer();
        this.parsedDirectory = (Int2ObjectSortedMap<DirectoryEntry>) parsedDirectory;
        this.directoryParsed = true;
        this.directoryCount = parsedDirectory.size();
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
     * Get a zero-copy ByteBuffer view of a field's data using a pre-fetched DirectoryEntry.
     * This avoids the cost of re-finding the entry.
     */
    public ByteBuffer getFieldBuffer(DirectoryEntry entry) throws ImprintException {
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
            return getOrParseDirectoryCount();
        } catch (ImprintException e) {
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

        int count = getOrParseDirectoryCount();
        if (count == 0)
            return null;

        // Advance buffer past the varint to get to the start of the entries.
        VarInt.decode(searchBuffer);
        int directoryStartPos = searchBuffer.position();

        int low = 0;
        int high = count - 1;

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
        var tailMap = parsedDirectory.tailMap(currentFieldId + 1);
        if (tailMap.isEmpty()) {
            return payload.limit();
        }
        return tailMap.get(tailMap.firstIntKey()).getOffset();
    }

    private int findNextOffsetInRawDirectory(int currentFieldId) throws ImprintException {
        var scanBuffer = directoryBuffer.duplicate();
        scanBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int count = getOrParseDirectoryCount();
        if (count == 0)
            return payload.limit();

        // Advance buffer past the varint to get to the start of the entries.
        VarInt.decode(scanBuffer);
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

            int count = getOrParseDirectoryCount(parseBuffer);
            this.parsedDirectory = new Int2ObjectAVLTreeMap<>();

            for (int i = 0; i < count; i++) {
                var entry = deserializeDirectoryEntry(parseBuffer);
                this.parsedDirectory.put(entry.getId() , entry);
            }

            this.directoryParsed = true;
        } catch (ImprintException e) {
            // This can happen with a corrupted directory.
            // In this case, we'll just have an empty (but valid) parsed directory.
            this.parsedDirectory = new Int2ObjectAVLTreeMap<>();
            this.directoryParsed = true; // Mark as parsed to avoid repeated errors
        }
    }

    private int getOrParseDirectoryCount() throws ImprintException {
        if (directoryCount != -1) {
            return directoryCount;
        }
        try {
            this.directoryCount = VarInt.decode(directoryBuffer.duplicate()).getValue();
        } catch (ImprintException e) {
            this.directoryCount = 0; // Cache as 0 on error
            throw e; // rethrow
        }
        return this.directoryCount;
    }

    private int getOrParseDirectoryCount(ByteBuffer buffer) throws ImprintException {
        // This method does not cache the count because it's used during parsing
        // where the buffer is transient. Caching is only for the instance's primary buffer.
        return VarInt.decode(buffer).getValue();
    }

    /**
     * Creates a read-only buffer containing the serialized directory.
     * The input collection does not need to be sorted.
     */
    static ByteBuffer createDirectoryBuffer(Collection<? extends DirectoryEntry> directory) {
        if (directory == null || directory.isEmpty()) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            VarInt.encode(0, buffer);
            buffer.flip();
            return buffer;
        }

        // Ensure sorted order for binary search compatibility.
        ArrayList<? extends DirectoryEntry> sortedDirectory;
        if (directory instanceof ArrayList && isSorted((ArrayList<? extends DirectoryEntry>)directory)) {
            sortedDirectory = (ArrayList<? extends DirectoryEntry>) directory;
        } else {
            sortedDirectory = new ArrayList<>(directory);
            sortedDirectory.sort(null);
        }

        int count = sortedDirectory.size();
        int size = VarInt.encodedLength(count) + (count * Constants.DIR_ENTRY_BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        VarInt.encode(count, buffer);
        for (DirectoryEntry entry : sortedDirectory) {
            serializeDirectoryEntry(entry, buffer);
        }

        buffer.flip();
        return buffer;
    }

    static ByteBuffer createDirectoryBufferFromMap(TreeMap<Integer, ? extends DirectoryEntry> directoryMap) {
        if (directoryMap == null || directoryMap.isEmpty()) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            VarInt.encode(0, buffer);
            buffer.flip();
            return buffer;
        }

        int count = directoryMap.size();
        int size = VarInt.encodedLength(count) + (count * Constants.DIR_ENTRY_BYTES);
        var buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        VarInt.encode(count, buffer);
        for (var entry : directoryMap.values()) {
            serializeDirectoryEntry(entry, buffer);
        }

        buffer.flip();
        return buffer;
    }

    static ByteBuffer createDirectoryBufferFromSortedMap(Int2ObjectSortedMap<? extends DirectoryEntry> directoryMap) {
        if (directoryMap == null || directoryMap.isEmpty()) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            VarInt.encode(0, buffer);
            buffer.flip();
            return buffer;
        }

        int count = directoryMap.size();
        int size = VarInt.encodedLength(count) + (count * Constants.DIR_ENTRY_BYTES);
        var buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        VarInt.encode(count, buffer);
        for (var entry : directoryMap.int2ObjectEntrySet()) {
            serializeDirectoryEntry(entry.getValue(), buffer);
        }

        buffer.flip();
        return buffer;
    }

    private static boolean isSorted(ArrayList<? extends DirectoryEntry> list) {
        for (int i = 0; i < list.size() - 1; i++) {
            if (list.get(i).getId() > list.get(i + 1).getId()) {
                return false;
            }
        }
        return true;
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