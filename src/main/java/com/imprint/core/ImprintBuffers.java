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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

    // Lazy-loaded directory state
    private List<DirectoryEntry> parsedDirectory;
    private boolean directoryParsed = false;
    private int directoryCount = -1; // Cached count to avoid repeated VarInt decoding

    /**
     * Creates buffers from raw data (used during deserialization).
     *
     * @param directoryBuffer Raw directory bytes including VarInt count and all entries.
     *                       Format: [VarInt count][Entry1][Entry2]...[EntryN]
     * @param payload Raw payload data containing all field values sequentially
     */
    public ImprintBuffers(ByteBuffer directoryBuffer, ByteBuffer payload) {
        this.directoryBuffer = directoryBuffer.asReadOnlyBuffer();
        this.payload = payload.asReadOnlyBuffer();
    }

    /**
     * Creates buffers from pre-parsed directory (used during construction).
     * This is more efficient when the directory is already known.
     *
     * @param directory Parsed directory entries, must be sorted by fieldId
     * @param payload Raw payload data containing all field values
     */
    public ImprintBuffers(List<DirectoryEntry> directory, ByteBuffer payload) {
        this.parsedDirectory = Collections.unmodifiableList(Objects.requireNonNull(directory));
        this.directoryParsed = true;
        this.directoryCount = directory.size();
        this.payload = payload.asReadOnlyBuffer();
        this.directoryBuffer = createDirectoryBuffer(directory);
    }

    /**
     * Get a zero-copy ByteBuffer view of a field's data.
     *
     * <p><strong>Buffer Positioning Logic:</strong></p>
     * <ol>
     * <li>Find the directory entry for the requested fieldId</li>
     * <li>Use entry.offset as start position in payload</li>
     * <li>Find end position by looking at next field's offset (or payload end)</li>
     * <li>Create a slice view: payload[startOffset:endOffset]</li>
     * </ol>
     *
     * @param fieldId The field identifier to retrieve
     * @return Zero-copy ByteBuffer positioned at field data, or null if field not found
     * @throws ImprintException if buffer bounds are invalid or directory is corrupted
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

        ByteBuffer fieldBuffer = payload.duplicate();
        fieldBuffer.position(startOffset).limit(endOffset);
        return fieldBuffer;
    }

    /**
     * Find a directory entry for the given field ID using the most efficient method.
     *
     * <p><strong>Search Strategy:</strong></p>
     * <ul>
     * <li>If directory is parsed: binary search on in-memory List&lt;DirectoryEntry&gt;</li>
     * <li>If directory is raw: binary search directly on raw bytes (faster for single lookups)</li>
     * </ul>
     * @param fieldId The field identifier to find
     * @return DirectoryEntry if found, null otherwise
     * @throws ImprintException if directory buffer is corrupted or truncated
     */
    public DirectoryEntry findDirectoryEntry(int fieldId) throws ImprintException {
        if (directoryParsed) {
            int index = findDirectoryIndexInParsed(fieldId);
            return index >= 0 ? parsedDirectory.get(index) : null;
        } else {
            return findFieldEntryInRawDirectory(fieldId);
        }
    }

    /**
     * Get the full directory, parsing it if necessary.
     *
     * <p><strong>Lazy Parsing Behavior:</strong></p>
     * <ul>
     * <li>First call: Parses entire directory from raw bytes into List&lt;DirectoryEntry&gt;</li>
     * <li>Subsequent calls: Returns cached parsed directory</li>
     * <li>Note - the method is not synchronized and assumes single-threaded usage.</li>
     * </ul>
     *
     * <p><strong>When to use:</strong> Call this if you need to access multiple fields
     * from the same record. For single field access, direct field getters are more efficient.</p>
     *
     * @return Immutable list of directory entries, sorted by fieldId
     */
    public List<DirectoryEntry> getDirectory() {
        ensureDirectoryParsed();
        return parsedDirectory;
    }

    /**
     * Get the directory count without fully parsing the directory.
     * <p>
     * This method avoids parsing the entire directory when only the count is needed.
     * <ol>
     * <li>Return cached count if available (directoryCount >= 0)</li>
     * <li>Return parsed directory size if directory is already parsed</li>
     * <li>Decode VarInt from raw buffer and cache the result</li>
     * </ol>
     *
     * <p><strong>VarInt Decoding:</strong> The count is stored as a VarInt at the beginning
     * of the directoryBuffer. This method reads just enough bytes to decode the count.</p>
     *
     * @return Number of fields in the directory, or 0 if decoding fails
     */
    public int getDirectoryCount() {
        if (directoryCount >= 0)
            return directoryCount;
        if (directoryParsed)
            return parsedDirectory.size();

        // Decode from buffer and cache
        try {
            var countBuffer = directoryBuffer.duplicate();
            directoryCount = VarInt.decode(countBuffer).getValue();
            return directoryCount;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Create a new buffer containing the serialized directory.
     *
     * <p><strong>Output Format:</strong></p>
     * <pre>
     * [VarInt count][DirectoryEntry 1][DirectoryEntry 2]...[DirectoryEntry N]
     * </pre>
     *
     * <p>Each DirectoryEntry is serialized as: [fieldId:2bytes][typeCode:1byte][offset:4bytes]</p>
     *
     *
     * @return New ByteBuffer containing the complete serialized directory
     */
    public ByteBuffer serializeDirectory() {
        ensureDirectoryParsed();
        return createDirectoryBuffer(parsedDirectory);
    }

    // ========== PRIVATE METHODS ==========

    /**
     * Binary search on raw directory bytes to find a specific field.
     *
     * <ol>
     * <li>Position buffer at start and decode VarInt count (cache for future use)</li>
     * <li>Calculate directory start position after VarInt</li>
     * <li>For binary search mid-point: entryPos = startPos + (mid * DIR_ENTRY_BYTES)</li>
     * <li>Read fieldId from calculated position (first 2 bytes of entry)</li>
     * <li>Compare fieldId and adjust search bounds</li>
     * <li>When found: reposition buffer and deserialize complete entry</li>
     * </ol>
     *
     * <p>All buffer positions are bounds-checked before access.</p>
     *
     * @param fieldId Field identifier to search for
     * @return Complete DirectoryEntry if found, null if not found
     * @throws ImprintException if buffer is truncated or corrupted
     */
    private DirectoryEntry findFieldEntryInRawDirectory(int fieldId) throws ImprintException {
        var searchBuffer = directoryBuffer.duplicate();
        searchBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Decode directory count (cache it)
        if (directoryCount < 0)
            directoryCount = VarInt.decode(searchBuffer).getValue();
        else
            VarInt.decode(searchBuffer); // Skip past the count

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
     *
     * @param fieldId Field identifier to find
     * @return Index of the field if found, or negative insertion point if not found
     */
    private int findDirectoryIndexInParsed(int fieldId) {
        if (!directoryParsed)
            return -1;
        int low = 0;
        int high = parsedDirectory.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midFieldId = parsedDirectory.get(mid).getId();
            if (midFieldId < fieldId)
                low = mid + 1;
            else if (midFieldId > fieldId)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    /**
     * Find the end offset for a field by looking at the next field's offset.
     *
     * <ul>
     * <li>Field data spans from: entry.offset to nextField.offset (exclusive)</li>
     * <li>Last field spans from: entry.offset to payload.limit()</li>
     * <li>This works because directory entries are sorted by fieldId</li>
     * </ul>
     *
     * <p><strong>Search Strategy:</strong></p>
     * <ul>
     * <li>If directory parsed: Use binary search result + 1 to get next entry</li>
     * <li>If directory raw: Scan raw entries until fieldId > currentFieldId</li>
     * </ul>
     *
     * @param entry The directory entry whose end offset we need to find
     * @return End offset (exclusive) for the field data
     * @throws ImprintException if directory scanning fails
     */
    private int findEndOffset(DirectoryEntry entry) throws ImprintException {
        if (directoryParsed) {
            int entryIndex = findDirectoryIndexInParsed(entry.getId());
            return (entryIndex + 1 < parsedDirectory.size()) ?
                    parsedDirectory.get(entryIndex + 1).getOffset() : payload.limit();
        } else
            return findNextOffsetInRawDirectory(entry.getId());
    }

    /**
     * Scan raw directory to find the next field's offset after currentFieldId.
     *
     * <ol>
     * <li>Position buffer after VarInt count</li>
     * <li>For each directory entry at position: startPos + (i * DIR_ENTRY_BYTES)</li>
     * <li>Read fieldId (first 2 bytes) and offset (bytes 3-6)</li>
     * <li>Return offset of first field where fieldId > currentFieldId</li>
     * <li>If no next field found, return payload.limit()</li>
     * </ol>
     *
     * @param currentFieldId Find the next field after this fieldId
     * @return Offset where the next field starts, or payload.limit() if this is the last field
     * @throws ImprintException if directory buffer is corrupted
     */
    private int findNextOffsetInRawDirectory(int currentFieldId) throws ImprintException {
        var scanBuffer = directoryBuffer.duplicate();
        scanBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int count = (directoryCount >= 0) ? directoryCount : VarInt.decode(scanBuffer).getValue();
        if (count == 0)
            return payload.limit();
        if (directoryCount >= 0)
            VarInt.decode(scanBuffer); // Skip count if cached

        int directoryStartPos = scanBuffer.position();

        for (int i = 0; i < count; i++) {
            int entryPos = directoryStartPos + (i * Constants.DIR_ENTRY_BYTES);

            if (entryPos + Constants.DIR_ENTRY_BYTES > scanBuffer.limit())
                return payload.limit();

            scanBuffer.position(entryPos);
            short fieldId = scanBuffer.getShort();
            scanBuffer.get(); // skip type
            int offset = scanBuffer.getInt();

            if (fieldId > currentFieldId)
                return offset;
        }

        return payload.limit();
    }

    /**
     * Parse the full directory if not already parsed.
     *
     * <ol>
     * <li>Duplicate directoryBuffer to avoid affecting original position</li>
     * <li>Set byte order to LITTLE_ENDIAN for consistent reading</li>
     * <li>Decode VarInt count and cache it</li>
     * <li>Read 'count' directory entries sequentially</li>
     * <li>Each entry: [fieldId:2bytes][typeCode:1byte][offset:4bytes]</li>
     * <li>Store as immutable list and mark as parsed</li>
     * </ol>
     *
     * <p><strong>Error Handling:</strong> If parsing fails, throws RuntimeException
     * since this indicates corrupted data that should never happen in normal operation.</p>
     *
     * <p>Will return immediately if directory has already been parsed.</p>
     */
    private void ensureDirectoryParsed() {
        if (directoryParsed)
            return;
        try {
            var parseBuffer = directoryBuffer.duplicate();
            parseBuffer.order(ByteOrder.LITTLE_ENDIAN);

            var countResult = VarInt.decode(parseBuffer);
            int count = countResult.getValue();
            this.directoryCount = count;

            var directory = new ArrayList<DirectoryEntry>(count);
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
     * Create directory buffer from parsed entries.
     *
     * <p><strong>Serialization Format:</strong></p>
     * <ol>
     * <li>Calculate buffer size: VarInt.encodedLength(count) + (count * DIR_ENTRY_BYTES)</li>
     * <li>Allocate ByteBuffer with LITTLE_ENDIAN byte order</li>
     * <li>Write VarInt count</li>
     * <li>Write each directory entry: [fieldId:2][typeCode:1][offset:4]</li>
     * <li>Flip buffer and return read-only view</li>
     * </ol>
     *
     * @param directory List of directory entries to serialize
     * @return Read-only ByteBuffer containing serialized directory, or empty buffer on error
     */
    private ByteBuffer createDirectoryBuffer(List<DirectoryEntry> directory) {
        try {
            int bufferSize = VarInt.encodedLength(directory.size()) +
                    (directory.size() * Constants.DIR_ENTRY_BYTES);
            var buffer = ByteBuffer.allocate(bufferSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            VarInt.encode(directory.size(), buffer);
            for (var entry : directory) {
                serializeDirectoryEntry(entry, buffer);
            }

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
    private void serializeDirectoryEntry(DirectoryEntry entry, ByteBuffer buffer) {
        buffer.putShort(entry.getId());
        buffer.put(entry.getTypeCode().getCode());
        buffer.putInt(entry.getOffset());
    }

    /**
     * Deserialize a single directory entry from the buffer.
     * Reads: [fieldId:2bytes][typeCode:1byte][offset:4bytes]
     *
     * @param buffer Buffer positioned at the start of a directory entry
     * @return Parsed DirectoryEntry
     * @throws ImprintException if buffer doesn't contain enough bytes
     */
    private DirectoryEntry deserializeDirectoryEntry(ByteBuffer buffer) throws ImprintException {
        if (buffer.remaining() < Constants.DIR_ENTRY_BYTES)
            throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for directory entry");

        short id = buffer.getShort();
        var typeCode = TypeCode.fromByte(buffer.get());
        int offset = buffer.getInt();

        return new DirectoryEntry(id, typeCode, offset);
    }
}