package com.imprint.ops;

import com.imprint.Constants;
import com.imprint.core.*;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.ImprintBuffer;
import com.imprint.util.VarInt;
import lombok.Value;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

@UtilityClass
public class ImprintOperations {

    /**
     * High-performance merge operation using SIMD-optimized operations.
     * Converts ByteBuffer inputs to ImprintBuffer for maximum performance.
     *
     * @param firstBuffer Complete serialized Imprint record
     * @param secondBuffer Complete serialized Imprint record
     * @return Merged record as serialized bytes
     * @throws ImprintException if merge fails
     */
    public static ByteBuffer mergeBytes(ByteBuffer firstBuffer, ByteBuffer secondBuffer) throws ImprintException {
        // Convert to ImprintBuffer for ultra-fast processing
        var first = ImprintBuffer.fromByteBuffer(firstBuffer);
        var second = ImprintBuffer.fromByteBuffer(secondBuffer);

        // Use ultra-fast implementation
        var result = mergeBytesUltraFast(first, second);

        // Convert back to ByteBuffer for API compatibility
        return result.toByteBuffer().asReadOnlyBuffer();
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
     * Ultra-fast SIMD-optimized merge taking ImprintBuffer inputs for maximum performance.
     * Eliminates ByteBuffer conversion overhead and enables end-to-end SIMD operations.
     */
    public static ImprintBuffer mergeBytesUltraFast(ImprintBuffer firstBuffer, ImprintBuffer secondBuffer) throws ImprintException {
        validateImprintBuffer(firstBuffer, "firstBuffer");
        validateImprintBuffer(secondBuffer, "secondBuffer");

        // Parse headers directly from ImprintBuffer (faster than ByteBuffer)
        var firstHeader = parseHeaderFromImprintBuffer(firstBuffer);
        var secondHeader = parseHeaderFromImprintBuffer(secondBuffer);

        // Extract sections using ImprintBuffer operations
        var firstSections = extractSectionsFromImprintBuffer(firstBuffer, firstHeader);
        var secondSections = extractSectionsFromImprintBuffer(secondBuffer, secondHeader);

        // Perform ultra-fast merge with full SIMD pipeline
        return mergeRawSectionsUltraFast(firstHeader, firstSections, secondSections);
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
     * Ultra-fast ImprintBuffer-native section extraction without ByteBuffer conversion.
     */
    private static class ImprintBufferSections {
        final ImprintBuffer directoryBuffer;
        final ImprintBuffer payloadBuffer;
        final int directoryCount;

        ImprintBufferSections(ImprintBuffer directoryBuffer, ImprintBuffer payloadBuffer, int directoryCount) {
            this.directoryBuffer = directoryBuffer;
            this.payloadBuffer = payloadBuffer;
            this.directoryCount = directoryCount;
        }
    }

    /**
     * Parse header directly from ImprintBuffer using SIMD-optimized reads.
     */
    private static Header parseHeaderFromImprintBuffer(ImprintBuffer buffer) throws ImprintException {
        buffer.position(0);

        // Read header components using ImprintBuffer's optimized operations
        byte magic = buffer.get();
        byte version = buffer.get();
        byte flags = buffer.get();
        int fieldSpaceId = buffer.getInt();
        int schemaHash = buffer.getInt();
        int payloadSize = buffer.getInt();

        if (magic != Constants.MAGIC) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, "Invalid magic byte");
        }
        if (version != Constants.VERSION) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, "Unsupported version: " + version);
        }

        return new Header(new Flags(flags), new SchemaId(fieldSpaceId, schemaHash), payloadSize);
    }

    /**
     * Extract directory and payload sections directly from ImprintBuffer.
     */
    private static ImprintBufferSections extractSectionsFromImprintBuffer(ImprintBuffer buffer, Header header) {
        buffer.position(Constants.HEADER_BYTES);

        // Read directory count using ImprintBuffer's VarInt operations
        int directoryCount = buffer.getVarInt();
        int directoryStart = buffer.position();
        int directorySize = directoryCount * Constants.DIR_ENTRY_BYTES;
        int payloadStart = directoryStart + directorySize;

        // Create sliced buffers for directory and payload (back to original working structure)
        var directoryBuffer = buffer.slice();
        directoryBuffer.limit(directorySize);

        buffer.position(payloadStart);
        var payloadBuffer = buffer.slice();
        payloadBuffer.limit(header.getPayloadSize());

        return new ImprintBufferSections(directoryBuffer, payloadBuffer, directoryCount);
    }

    /**
     * Ultra-fast merge implementation using growable buffer with UNSAFE.
     */
    private static ImprintBuffer mergeRawSectionsUltraFast(Header firstHeader, ImprintBufferSections firstSections, ImprintBufferSections secondSections) {

        // Estimate reasonable initial size
        int maxFields = firstSections.directoryCount + secondSections.directoryCount;
        int estimatedDirectorySize = ImprintRecord.calculateDirectorySize(maxFields);
        int estimatedPayloadSize = firstSections.payloadBuffer.remaining() + secondSections.payloadBuffer.remaining();
        int estimatedTotalSize = Constants.HEADER_BYTES + estimatedDirectorySize + estimatedPayloadSize;

        // Create growable buffer
        var output = ImprintBuffer.growable(estimatedTotalSize);

        // Reserve space for header and directory - write payload first
        int headerAndDirSize = Constants.HEADER_BYTES + estimatedDirectorySize;
        output.position(headerAndDirSize);

        // Merge fields directly into growable buffer
        var mergedEntries = new ArrayList<RawDirectoryEntry>(maxFields);
        int actualPayloadSize = mergeFieldsDirectToBuffer(output, firstSections, secondSections, mergedEntries);

        // Calculate actual directory size
        int actualDirectorySize = ImprintRecord.calculateDirectorySize(mergedEntries.size());
        int actualTotalSize = Constants.HEADER_BYTES + actualDirectorySize + actualPayloadSize;

        // If directory size changed, move payload using UNSAFE
        int payloadStartPos = Constants.HEADER_BYTES + actualDirectorySize;
        if (actualDirectorySize != estimatedDirectorySize) {
            output.moveMemory(headerAndDirSize, payloadStartPos, actualPayloadSize);
        }

        // Write header and directory at the beginning
        output.position(0);
        output.putUnsafeByte(Constants.MAGIC);
        output.putUnsafeByte(Constants.VERSION);
        output.putUnsafeByte(firstHeader.getFlags().getValue());
        output.putUnsafeInt(firstHeader.getSchemaId().getFieldSpaceId());
        output.putUnsafeInt(firstHeader.getSchemaId().getSchemaHash());
        output.putUnsafeInt(actualPayloadSize);

        // Write directory
        output.putVarInt(mergedEntries.size());
        for (var entry : mergedEntries) {
            output.putUnsafeShort(entry.fieldId);
            output.putUnsafeByte(entry.typeCode);
            output.putUnsafeInt(entry.offset);
        }

        // Final buffer ready!
        output.position(0);
        output.limit(actualTotalSize);
        return output;
    }

    /**
     * Ultra-fast field merging directly into growable buffer with zero intermediate allocations.
     */
    private static int mergeFieldsDirectToBuffer(ImprintBuffer output, ImprintBufferSections firstSections, ImprintBufferSections secondSections, List<RawDirectoryEntry> mergedEntries) {

        var firstIter = new ImprintBufferDirectoryIterator(firstSections.directoryBuffer);
        var secondIter = new ImprintBufferDirectoryIterator(secondSections.directoryBuffer);

        int currentOffset = 0;
        int initialPosition = output.position();

        var firstEntry = firstIter.hasNext() ? firstIter.next() : null;
        var secondEntry = secondIter.hasNext() ? secondIter.next() : null;

        // Ultra-fast streaming merge directly into growable buffer
        while (firstEntry != null || secondEntry != null) {
            RawDirectoryEntry selectedEntry;
            ImprintBuffer sourcePayload;
            ImprintBufferDirectoryIterator sourceIter;

            if (firstEntry != null && (secondEntry == null || firstEntry.fieldId <= secondEntry.fieldId)) {
                selectedEntry = firstEntry;
                sourcePayload = firstSections.payloadBuffer;
                sourceIter = firstIter;

                if (secondEntry != null && firstEntry.fieldId == secondEntry.fieldId) {
                    secondEntry = secondIter.hasNext() ? secondIter.next() : null;
                }

                firstEntry = firstIter.hasNext() ? firstIter.next() : null;
            } else {
                selectedEntry = secondEntry;
                sourcePayload = secondSections.payloadBuffer;
                sourceIter = secondIter;

                secondEntry = secondIter.hasNext() ? secondIter.next() : null;
            }

            // Ultra-fast field copying using ImprintBuffer-to-ImprintBuffer transfer
            int fieldSize = getFieldSizeUltraFast(sourcePayload, selectedEntry, sourceIter);
            copyFieldUltraFast(output, sourcePayload, selectedEntry.offset, fieldSize);

            mergedEntries.add(new RawDirectoryEntry(selectedEntry.fieldId, selectedEntry.typeCode, currentOffset));
            currentOffset += fieldSize;
        }

        return output.position() - initialPosition;
    }

    /**
     * Optimized field size calculation without object allocation.
     */
    private static int getFieldSizeOptimized(ImprintBuffer payload, int startOffset, ImprintBufferDirectoryIterator iterator) {
        int endOffset = iterator.getNextEntryOffset(payload.remaining());
        return endOffset - startOffset;
    }
    
    /**
     * Legacy method for backwards compatibility.
     */
    private static int getFieldSizeUltraFast(ImprintBuffer payload, RawDirectoryEntry entry, ImprintBufferDirectoryIterator iterator) {
        return getFieldSizeOptimized(payload, entry.offset, iterator);
    }

    /**
     * Ultra-fast field copying using ImprintBuffer-to-ImprintBuffer bulk transfer.
     */
    private static void copyFieldUltraFast(ImprintBuffer output, ImprintBuffer source, int offset, int size) {
        if (size <= 0) return;
        // Direct ImprintBuffer-to-ImprintBuffer copy using Unsafe operations
        output.putBytes(source.array(), source.arrayOffset() + offset, size);
    }

    /**
     * Simple directory iterator using ImprintBuffer operations.
     */
    private static class ImprintBufferDirectoryIterator {
        private final ImprintBuffer buffer;
        private final int totalCount;
        private int currentIndex;

        ImprintBufferDirectoryIterator(ImprintBuffer directoryBuffer) {
            this.buffer = directoryBuffer;
            this.buffer.position(0);
            this.totalCount = directoryBuffer.remaining() / Constants.DIR_ENTRY_BYTES;
            this.currentIndex = 0;
        }

        boolean hasNext() {
            return currentIndex < totalCount;
        }

        RawDirectoryEntry next() {
            if (!hasNext()) {
                throw new RuntimeException("No more directory entries");
            }

            short fieldId = buffer.getShort();
            byte typeCode = buffer.get();
            int offset = buffer.getInt();
            currentIndex++;
            
            return new RawDirectoryEntry(fieldId, typeCode, offset);
        }

        int getNextEntryOffset(int fallbackOffset) {
            if (currentIndex >= totalCount) {
                return fallbackOffset;
            }

            int savedPos = buffer.position();
            buffer.position(savedPos + 3); // Skip fieldId and typeCode
            int offset = buffer.getInt();
            buffer.position(savedPos);
            return offset;
        }
    }

    /**
     * Overloaded validation for ImprintBuffer.
     */
    private static void validateImprintBuffer(ImprintBuffer buffer, String paramName) throws ImprintException {
        if (buffer == null) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " cannot be null");
        }

        if (buffer.remaining() < Constants.HEADER_BYTES) {
            throw new ImprintException(ErrorType.INVALID_BUFFER,
                    paramName + " too small to contain valid Imprint header (minimum " + Constants.HEADER_BYTES + " bytes)");
        }

        // Check magic and version using ImprintBuffer operations
        int savedPos = buffer.position();
        byte magic = buffer.get();
        byte version = buffer.get();
        buffer.position(savedPos);

        if (magic != Constants.MAGIC) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " does not contain valid Imprint magic byte");
        }
        if (version != Constants.VERSION) {
            throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " contains unsupported Imprint version: " + version);
        }
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
     * Simple directory entry container.
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
