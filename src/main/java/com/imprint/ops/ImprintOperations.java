package com.imprint.ops;

import com.imprint.Constants;
import com.imprint.core.*;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.VarInt;
import lombok.Value;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

@UtilityClass
public class ImprintOperations {

    /**
     * Pure bytes-to-bytes merge operation that avoids all object creation.
     * Performs merge directly on serialized Imprint record buffers.
     * 
     * @param firstBuffer Complete serialized Imprint record
     * @param secondBuffer Complete serialized Imprint record  
     * @return Merged record as serialized bytes
     * @throws ImprintException if merge fails
     */
    public static ByteBuffer mergeBytes(ByteBuffer firstBuffer, ByteBuffer secondBuffer) throws ImprintException {
        validateImprintBuffer(firstBuffer, "firstBuffer");
        validateImprintBuffer(secondBuffer, "secondBuffer");

        // TODO possible could work directly on the originals but duplicate makes the mark values and offsets easy to reason about
        // duplicates to avoid affecting original positions, we'll need to preserve at least one side
        var first = firstBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        var second = secondBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        
        // Parse headers  
        var firstHeader = parseHeaderOnly(first);
        var secondHeader = parseHeaderOnly(second);
        
        // Extract directory and payload sections
        var firstSections = extractSections(first, firstHeader);
        var secondSections = extractSections(second, secondHeader);
        
        // Perform raw merge
        return mergeRawSections(firstHeader, firstSections, secondSections);
    }

    /**
     * Parse just the header without advancing buffer past it
     */
    private static Header parseHeaderOnly(ByteBuffer buffer) throws ImprintException {
        return ImprintRecord.parseHeaderFromBuffer(buffer);
    }
    
    /**
     * Extract directory and payload sections from a buffer
     */
    private static ImprintRecord.BufferSections extractSections(ByteBuffer buffer, Header header) throws ImprintException {
        return ImprintRecord.extractBufferSections(buffer, header);
    }
    
    /**
     * Merge raw directory and payload sections without object creation
     * Assumes incoming streams are already both sorted from the serialization process
     */
    private static ByteBuffer mergeRawSections(Header firstHeader, ImprintRecord.BufferSections firstSections, ImprintRecord.BufferSections secondSections) throws ImprintException {
        // Prepare directory iterators
        var firstDirIter = new RawDirectoryIterator(firstSections.directoryBuffer);
        var secondDirIter = new RawDirectoryIterator(secondSections.directoryBuffer);
        
        // Pre-allocate - worst case is sum of both directory counts
        int maxEntries = firstSections.directoryCount + secondSections.directoryCount;
        var mergedDirectoryEntries = new ArrayList<RawDirectoryEntry>(maxEntries);
        var mergedChunks = new ArrayList<ByteBuffer>(maxEntries);

        int totalMergedPayloadSize = 0;
        int currentMergedOffset = 0;
        
        var firstEntry = firstDirIter.hasNext() ? firstDirIter.next() : null;
        var secondEntry = secondDirIter.hasNext() ? secondDirIter.next() : null;
        
        // Merge directories and collect payload chunks
        while (firstEntry != null || secondEntry != null) {
            RawDirectoryEntry currentEntry;
            ByteBuffer sourcePayload;
            
            if (firstEntry != null && (secondEntry == null || firstEntry.fieldId <= secondEntry.fieldId)) {
                // Take from first
                currentEntry = firstEntry;
                sourcePayload = getFieldPayload(firstSections.payloadBuffer, firstEntry, firstDirIter);
                
                // Skip duplicate in second if present
                if (secondEntry != null && firstEntry.fieldId == secondEntry.fieldId)
                    secondEntry = secondDirIter.hasNext() ? secondDirIter.next() : null;

                firstEntry = firstDirIter.hasNext() ? firstDirIter.next() : null;
            } else {
                // Take from second
                currentEntry = secondEntry;
                sourcePayload = getFieldPayload(secondSections.payloadBuffer, secondEntry, secondDirIter);
                secondEntry = secondDirIter.hasNext() ? secondDirIter.next() : null;
            }
            
            // Add to merged directory with adjusted offset
            var adjustedEntry = new RawDirectoryEntry(currentEntry.fieldId, currentEntry.typeCode, currentMergedOffset);
            mergedDirectoryEntries.add(adjustedEntry);
            
            // Collect payload chunk
            mergedChunks.add(sourcePayload.duplicate());
            currentMergedOffset += sourcePayload.remaining();
            totalMergedPayloadSize += sourcePayload.remaining();
        }
        
        // Build final merged buffer
        return buildSerializedBuffer(firstHeader, mergedDirectoryEntries, mergedChunks, totalMergedPayloadSize);
    }
    
    /**
     * Get payload bytes for a specific field using iterator state
     */
    private static ByteBuffer getFieldPayload(ByteBuffer payload, RawDirectoryEntry entry, RawDirectoryIterator iterator) {
        int startOffset = entry.offset;
        int endOffset = iterator.getNextEntryOffset(payload.limit());

        var fieldPayload = payload.duplicate();
        fieldPayload.position(startOffset);
        fieldPayload.limit(endOffset);
        return fieldPayload.slice();
    }
    

    /**
     * Pure bytes-to-bytes projection operation that avoids all object creation.
     * Projects a subset of fields directly from a serialized Imprint record.
     * 
     * @param sourceBuffer Complete serialized Imprint record
     * @param fieldIds Array of field IDs to include in projection
     * @return Projected record as serialized bytes
     * @throws ImprintException if projection fails
     */
    public static ByteBuffer projectBytes(ByteBuffer sourceBuffer, int... fieldIds) throws ImprintException {
        validateImprintBuffer(sourceBuffer, "sourceBuffer");
        
        if (fieldIds == null || fieldIds.length == 0) {
            return createEmptyRecordBytes();
        }

        var sortedFieldIds = fieldIds.clone();
        Arrays.sort(sortedFieldIds);
        
        // Duplicate avoids affecting original position which we'll need later
        var source = sourceBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        
        // Parse header
        var header = parseHeaderOnly(source);
        
        // Extract sections
        var sections = extractSections(source, header);
        
        // Perform raw projection
        return projectRawSections(header, sections, sortedFieldIds);
    }

    /**
     * Project raw sections without object creation using optimized merge algorithm.
     */
    private static ByteBuffer projectRawSections(Header originalHeader, ImprintRecord.BufferSections sections, int[] sortedRequestedFields) throws ImprintException {
        
        if (sortedRequestedFields.length == 0) {
            return buildSerializedBuffer(originalHeader, new RawDirectoryEntry[0], new ByteBuffer[0]);
        }
        
        // Use pre-sized ArrayLists to avoid System.arraycopy but still be efficient
        var projectedEntries = new ArrayList<RawDirectoryEntry>(sortedRequestedFields.length);
        var payloadChunks = new ArrayList<ByteBuffer>(sortedRequestedFields.length);
        int totalProjectedPayloadSize = 0;
        int currentOffset = 0;
        int requestedIndex = 0;
        
        // Optimize: Cache payload buffer reference to avoid getter calls
        var payloadBuffer = sections.payloadBuffer;
        
        // Merge algorithm: two-pointer approach through sorted sequences
        var dirIterator = new RawDirectoryIterator(sections.directoryBuffer);
        RawDirectoryEntry currentEntry = dirIterator.hasNext() ? dirIterator.next() : null;
        
        while (currentEntry != null && requestedIndex < sortedRequestedFields.length) {
            int fieldId = currentEntry.fieldId;
            int targetFieldId = sortedRequestedFields[requestedIndex];
            
            if (fieldId == targetFieldId) {
                var fieldPayload = getFieldPayload(payloadBuffer, currentEntry, dirIterator);
                
                // Add to projection with adjusted offset
                projectedEntries.add(new RawDirectoryEntry(currentEntry.fieldId, currentEntry.typeCode, currentOffset));
                
                // Collect payload chunk here  - fieldPayload should already sliced
                payloadChunks.add(fieldPayload);
                
                int payloadSize = fieldPayload.remaining();
                currentOffset += payloadSize;
                totalProjectedPayloadSize += payloadSize;
                
                // Advance both pointers - handle dupes by advancing to next unique field hopefully
                do {
                    requestedIndex++;
                } while (requestedIndex < sortedRequestedFields.length && sortedRequestedFields[requestedIndex] == targetFieldId);
                
                currentEntry = dirIterator.hasNext() ? dirIterator.next() : null;
            } else if (fieldId < targetFieldId) {
                // Directory field is smaller, advance directory pointer
                currentEntry = dirIterator.hasNext() ? dirIterator.next() : null;
            } else {
                // fieldId > targetFieldId - implies requested field isn't in the directory so advance requested pointer
                requestedIndex++;
            }
        }
        
        return buildSerializedBuffer(originalHeader, projectedEntries, payloadChunks, totalProjectedPayloadSize);
    }

    /**
     * Build a serialized Imprint record buffer from header, directory entries, and payload chunks.
     */
    private static ByteBuffer buildSerializedBuffer(Header originalHeader, RawDirectoryEntry[] directoryEntries, ByteBuffer[] payloadChunks) {
        return buildSerializedBuffer(originalHeader, Arrays.asList(directoryEntries), Arrays.asList(payloadChunks), 0);
    }
    
    private static ByteBuffer buildSerializedBuffer(Header originalHeader, List<RawDirectoryEntry> directoryEntries, List<ByteBuffer> payloadChunks, int totalPayloadSize) {
        int directorySize = ImprintRecord.calculateDirectorySize(directoryEntries.size());
        int totalSize = Constants.HEADER_BYTES + directorySize + totalPayloadSize;
        var finalBuffer = ByteBuffer.allocate(totalSize);
        finalBuffer.order(ByteOrder.LITTLE_ENDIAN);
        
        // header (preserve original schema)
        finalBuffer.put(Constants.MAGIC);
        finalBuffer.put(Constants.VERSION);
        finalBuffer.put(originalHeader.getFlags().getValue());
        finalBuffer.putInt(originalHeader.getSchemaId().getFieldSpaceId());
        finalBuffer.putInt(originalHeader.getSchemaId().getSchemaHash());
        finalBuffer.putInt(totalPayloadSize);
        
        // directory
        VarInt.encode(directoryEntries.size(), finalBuffer);
        for (var entry : directoryEntries) {
            finalBuffer.putShort(entry.fieldId);
            finalBuffer.put(entry.typeCode);
            finalBuffer.putInt(entry.offset);
        }
        
        // payload
        for (var chunk : payloadChunks)
            finalBuffer.put(chunk);

        finalBuffer.flip();
        return finalBuffer.asReadOnlyBuffer();
    }
    
    
    /**
     * Create an empty record as serialized bytes
     */
    private static ByteBuffer createEmptyRecordBytes() {
        // header + empty directory + empty payload
        var buffer = ByteBuffer.allocate(Constants.HEADER_BYTES + 1); // +1 for varint 0
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        // header for empty record
        buffer.put(Constants.MAGIC);
        buffer.put(Constants.VERSION);
        buffer.put((byte) 0x01);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.putInt(0);
        
        // empty directory
        VarInt.encode(0, buffer);
        
        buffer.flip();
        return buffer.asReadOnlyBuffer();
    }

    /**
     * Validates that a ByteBuffer contains valid Imprint data by checking magic bytes and basic structure.
     * 
     * @param buffer Buffer to validate
     * @param paramName Parameter name for error messages
     * @throws ImprintException if buffer is invalid
     */
    private static void validateImprintBuffer(ByteBuffer buffer, String paramName) throws ImprintException {
        if (buffer == null) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " cannot be null");
        }
        
        if (buffer.remaining() < Constants.HEADER_BYTES) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, 
                paramName + " too small to contain valid Imprint header (minimum " + Constants.HEADER_BYTES + " bytes)");
        }

        // Check invariants without advancing buffer position
        var duplicate = buffer.duplicate();
        byte magic = duplicate.get();
        byte version = duplicate.get();
        if (magic != Constants.MAGIC)
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " does not contain valid Imprint magic byte");
        if (version != Constants.VERSION)
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " contains unsupported Imprint version: " + version);
    }

    /**
     * Directory entry container used for raw byte operations
     */
    @Value
    private static class RawDirectoryEntry {
        short fieldId;
        byte typeCode;
        int offset;
    }

    /**
     * Iterator that parses directory entries directly from raw bytes
     */
    private static class RawDirectoryIterator {
        private final ByteBuffer buffer;
        private final int totalCount;
        private final int directoryStartPos;
        private int currentIndex;
        
        RawDirectoryIterator(ByteBuffer directoryBuffer) throws ImprintException {
            this.buffer = directoryBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            
            // Read count and advance to first entry
            var countResult = VarInt.decode(buffer);
            this.totalCount = countResult.getValue();
            this.directoryStartPos = buffer.position();
            this.currentIndex = 0;
        }
        
        boolean hasNext() {
            return currentIndex < totalCount;
        }
        
        RawDirectoryEntry next() throws ImprintException {
            if (!hasNext())
                throw new RuntimeException("No more directory entries");

            if (buffer.remaining() < Constants.DIR_ENTRY_BYTES)
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Not enough bytes for directory entry");

            short fieldId = buffer.getShort();
            byte typeCode = buffer.get();
            int offset = buffer.getInt();
            
            currentIndex++;
            return new RawDirectoryEntry(fieldId, typeCode, offset);
        }
        
        /**
         * Get the offset of the next entry without state overhead.
         * Returns the provided fallback if this is the last entry.
         */
        int getNextEntryOffset(int fallbackOffset) {
            if (currentIndex >= totalCount)
                return fallbackOffset;

            // Calculate position of next entry directly
            int nextEntryPos = directoryStartPos + (currentIndex * Constants.DIR_ENTRY_BYTES);
            
            // Bounds check - optimized to single comparison
            if (nextEntryPos + 7 > buffer.limit()) { // DIR_ENTRY_BYTES = 7
                return fallbackOffset;
            }
            
            // Read just the offset field (skip fieldId and typeCode)
            return buffer.getInt(nextEntryPos + 3); // 2 bytes fieldId + 1 byte typeCode = 3 offset
        }
    }
}
