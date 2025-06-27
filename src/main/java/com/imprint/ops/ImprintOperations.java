package com.imprint.ops;

import com.imprint.Constants;
import com.imprint.core.*;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.util.ImprintBuffer;
import lombok.Value;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.*;

@UtilityClass
public class ImprintOperations {

    // ========== BACKWARD COMPATIBILITY CONVENIENCE METHODS ==========

    public static ImprintBuffer mergeBytes(ImprintBuffer firstBuffer, ImprintBuffer secondBuffer) throws ImprintException {
        return Merge.mergeBytes(firstBuffer, secondBuffer);
    }


    /**
     * High-performance projection taking ImprintBuffer inputs for maximum performance.
     */
    public static ImprintBuffer projectBytes(ImprintBuffer sourceBuffer, int... fieldIds) throws ImprintException {
        return Project.projectBytes(sourceBuffer, fieldIds);
    }

    
    /**
     * Shared utilities and data structures used by both Merge and Project operations.
     */
    static class Core {
        
        /**
         * Container for separated directory and payload buffer sections.
         */
        static class ImprintBufferSections {
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
         * Simple directory entry container.
         */
        @Value
        static class RawDirectoryEntry {
            short fieldId;
            byte typeCode;
            int offset;
        }

        /**
         * Simple directory iterator using ImprintBuffer operations.
         */
        static class ImprintBufferDirectoryIterator {
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
         * Parse header directly from ImprintBuffer using SIMD-optimized reads.
         */
        static Header parseHeaderFromImprintBuffer(ImprintBuffer buffer) throws ImprintException {
            // Use duplicate to avoid modifying the original buffer
            var headerBuffer = buffer.duplicate();
            headerBuffer.position(0);

            // Read header components using ImprintBuffer's optimized operations
            byte magic = headerBuffer.get();
            byte version = headerBuffer.get();
            byte flags = headerBuffer.get();
            int fieldSpaceId = headerBuffer.getInt();
            int schemaHash = headerBuffer.getInt();
            int payloadSize = headerBuffer.getInt();

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
        static ImprintBufferSections extractSectionsFromImprintBuffer(ImprintBuffer buffer, Header header) {
            // Use duplicate to avoid modifying the original buffer
            var workingBuffer = buffer.duplicate();
            workingBuffer.position(Constants.HEADER_BYTES);

            // Read directory count using ImprintBuffer's VarInt operations
            int directoryCount = workingBuffer.getVarInt();
            int directoryStart = workingBuffer.position();
            int directorySize = directoryCount * Constants.DIR_ENTRY_BYTES;
            int payloadStart = directoryStart + directorySize;

            // Create sliced buffers for directory and payload
            var directoryBuffer = workingBuffer.slice();
            directoryBuffer.limit(directorySize);

            workingBuffer.position(payloadStart);
            var payloadBuffer = workingBuffer.slice();
            payloadBuffer.limit(header.getPayloadSize());

            return new ImprintBufferSections(directoryBuffer, payloadBuffer, directoryCount);
        }

        /**
         * Validates that an ImprintBuffer contains valid Imprint data.
         */
        static void validateImprintBuffer(ImprintBuffer buffer, String paramName) throws ImprintException {
            if (buffer == null) {
                throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " cannot be null");
            }

            // Debug: Log validation details for small buffers
            if (buffer.remaining() <= Constants.HEADER_BYTES) {
                System.err.println("DEBUG: validateImprintBuffer() - " + paramName + " validation details:");
                System.err.println("  buffer.remaining()=" + buffer.remaining());
                System.err.println("  buffer.position()=" + buffer.position());
                System.err.println("  buffer.limit()=" + buffer.limit());
                System.err.println("  buffer.capacity()=" + buffer.capacity());
                System.err.println("  Constants.HEADER_BYTES=" + Constants.HEADER_BYTES);
            }

            if (buffer.remaining() < Constants.HEADER_BYTES) {
                throw new ImprintException(ErrorType.INVALID_BUFFER,
                        paramName + " too small to contain valid Imprint header (minimum " + Constants.HEADER_BYTES + " bytes)");
            }

            // Check magic and version using ImprintBuffer operations without modifying original buffer
            var validationBuffer = buffer.duplicate();
            validationBuffer.position(0);
            byte magic = validationBuffer.get();
            byte version = validationBuffer.get();

            if (magic != Constants.MAGIC) {
                throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " does not contain valid Imprint magic byte");
            }
            if (version != Constants.VERSION) {
                throw new ImprintException(ErrorType.INVALID_BUFFER, paramName + " contains unsupported Imprint version: " + version);
            }
        }

        /**
         * Create an empty record as ImprintBuffer.
         */
        static ImprintBuffer createEmptyRecordBytes() {
            // header + empty directory + empty payload
            var buffer = ImprintBuffer.growable(Constants.HEADER_BYTES + 1); // +1 for varint 0

            // header for empty record
            buffer.putUnsafeByte(Constants.MAGIC);
            buffer.putUnsafeByte(Constants.VERSION);
            buffer.putUnsafeByte((byte) 0x01);
            buffer.putUnsafeInt(0);
            buffer.putUnsafeInt(0);
            buffer.putUnsafeInt(0);

            // empty directory
            buffer.putVarInt(0);

            // Set final position and limit
            int finalSize = buffer.position();
            buffer.position(0);
            buffer.limit(finalSize);
            return buffer;
        }

        /**
         * Optimized field size calculation without object allocation.
         */
        static int getFieldSizeOptimized(ImprintBuffer payload, int startOffset, ImprintBufferDirectoryIterator iterator) {
            int endOffset = iterator.getNextEntryOffset(payload.remaining());
            return endOffset - startOffset;
        }

        /**
         * High-performance field copying using ImprintBuffer-to-ImprintBuffer bulk transfer.
         */
        static void copyField(ImprintBuffer output, ImprintBuffer source, int offset, int size) {
            if (size <= 0) return;
            // Direct ImprintBuffer-to-ImprintBuffer copy using Unsafe operations
            output.putBytes(source.array(), source.arrayOffset() + offset, size);
        }

        /**
         * Build and finalize an ImprintBuffer with header, directory, and payload.
         * Handles memory layout optimization and header/directory writing.
         */
        static ImprintBuffer buildFinalBuffer(Header header, List<RawDirectoryEntry> entries, 
                                              ImprintBuffer output, int estimatedDirectorySize, 
                                              int actualPayloadSize, int headerAndDirSize) {
            
            // Calculate actual directory size
            int actualDirectorySize = ImprintRecord.calculateDirectorySize(entries.size());
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
            output.putUnsafeByte(header.getFlags().getValue());
            output.putUnsafeInt(header.getSchemaId().getFieldSpaceId());
            output.putUnsafeInt(header.getSchemaId().getSchemaHash());
            output.putUnsafeInt(actualPayloadSize);

            // Write directory
            output.putVarInt(entries.size());
            for (var entry : entries) {
                output.putUnsafeShort(entry.fieldId);
                output.putUnsafeByte(entry.typeCode);
                output.putUnsafeInt(entry.offset);
            }

            // Final buffer ready!
            output.position(0);
            output.limit(actualTotalSize);
            return output;
        }
    }

    // ========== MERGE OPERATIONS ==========
    
    public static class Merge {
        
        /**
         * High-performance merge operation using SIMD-optimized operations.
         * Converts ByteBuffer inputs to ImprintBuffer for maximum performance.
         */
        public static ByteBuffer mergeBytes(ByteBuffer firstBuffer, ByteBuffer secondBuffer) throws ImprintException {
            // Convert to ImprintBuffer for high-performance processing
            var first = ImprintBuffer.fromByteBuffer(firstBuffer);
            var second = ImprintBuffer.fromByteBuffer(secondBuffer);

            // Use high-performance implementation
            var result = mergeBytes(first, second);

            // Convert back to ByteBuffer for API compatibility
            return result.toByteBuffer().asReadOnlyBuffer();
        }

        /**
         * High-performance merge taking ImprintBuffer inputs for maximum performance.
         * Eliminates ByteBuffer conversion overhead and enables end-to-end operations.
         */
        public static ImprintBuffer mergeBytes(ImprintBuffer firstBuffer, ImprintBuffer secondBuffer) throws ImprintException {
            // Debug: Log buffer state before validation for small buffers
            if (firstBuffer.remaining() <= 20) {
                System.err.println("DEBUG: mergeBytes() - firstBuffer before validation:");
                System.err.println("  firstBuffer.remaining()=" + firstBuffer.remaining());
                System.err.println("  firstBuffer.position()=" + firstBuffer.position());
                System.err.println("  firstBuffer.limit()=" + firstBuffer.limit());
            }
            
            Core.validateImprintBuffer(firstBuffer, "firstBuffer");
            Core.validateImprintBuffer(secondBuffer, "secondBuffer");

            // Debug: Log buffer state after validation
            if (firstBuffer.remaining() <= 20) {
                System.err.println("DEBUG: mergeBytes() - firstBuffer after validation:");
                System.err.println("  firstBuffer.remaining()=" + firstBuffer.remaining());
                System.err.println("  firstBuffer.position()=" + firstBuffer.position());
                System.err.println("  firstBuffer.limit()=" + firstBuffer.limit());
            }

            // Parse headers directly from ImprintBuffer (faster than ByteBuffer)
            var firstHeader = Core.parseHeaderFromImprintBuffer(firstBuffer);
            var secondHeader = Core.parseHeaderFromImprintBuffer(secondBuffer);

            // Debug: Log buffer state after header parsing
            if (firstBuffer.remaining() <= 20) {
                System.err.println("DEBUG: mergeBytes() - firstBuffer after header parsing:");
                System.err.println("  firstBuffer.remaining()=" + firstBuffer.remaining());
                System.err.println("  firstBuffer.position()=" + firstBuffer.position());
                System.err.println("  firstBuffer.limit()=" + firstBuffer.limit());
            }

            // Extract sections using ImprintBuffer operations
            var firstSections = Core.extractSectionsFromImprintBuffer(firstBuffer, firstHeader);
            var secondSections = Core.extractSectionsFromImprintBuffer(secondBuffer, secondHeader);

            // Debug: Log buffer state after section extraction
            if (firstBuffer.remaining() <= 20) {
                System.err.println("DEBUG: mergeBytes() - firstBuffer after section extraction:");
                System.err.println("  firstBuffer.remaining()=" + firstBuffer.remaining());
                System.err.println("  firstBuffer.position()=" + firstBuffer.position());
                System.err.println("  firstBuffer.limit()=" + firstBuffer.limit());
            }

            // Perform high-performance merge
            return mergeRawSections(firstHeader, firstSections, secondSections);
        }

        /**
         * High-performance merge implementation using growable buffer with UNSAFE operations.
         */
        private static ImprintBuffer mergeRawSections(Header firstHeader, Core.ImprintBufferSections firstSections, Core.ImprintBufferSections secondSections) {

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
            var mergedEntries = new ArrayList<Core.RawDirectoryEntry>(maxFields);
            int actualPayloadSize = mergeFieldsDirectToBuffer(output, firstSections, secondSections, mergedEntries);

            // Build and finalize the buffer
            return Core.buildFinalBuffer(firstHeader, mergedEntries, output,
                                                 estimatedDirectorySize, actualPayloadSize, headerAndDirSize);
        }

        private static int mergeFieldsDirectToBuffer(ImprintBuffer output, Core.ImprintBufferSections firstSections, Core.ImprintBufferSections secondSections, List<Core.RawDirectoryEntry> mergedEntries) {

            var firstIter = new Core.ImprintBufferDirectoryIterator(firstSections.directoryBuffer);
            var secondIter = new Core.ImprintBufferDirectoryIterator(secondSections.directoryBuffer);

            int currentOffset = 0;
            int initialPosition = output.position();

            var firstEntry = firstIter.hasNext() ? firstIter.next() : null;
            var secondEntry = secondIter.hasNext() ? secondIter.next() : null;

            // High-performance streaming merge directly into growable buffer
            while (firstEntry != null || secondEntry != null) {
                Core.RawDirectoryEntry selectedEntry;
                ImprintBuffer sourcePayload;
                Core.ImprintBufferDirectoryIterator sourceIter;

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

                // High-performance field copying using ImprintBuffer-to-ImprintBuffer transfer
                int fieldSize = Core.getFieldSizeOptimized(sourcePayload, selectedEntry.offset, sourceIter);
                Core.copyField(output, sourcePayload, selectedEntry.offset, fieldSize);

                mergedEntries.add(new Core.RawDirectoryEntry(selectedEntry.fieldId, selectedEntry.typeCode, currentOffset));
                currentOffset += fieldSize;
            }

            return output.position() - initialPosition;
        }
    }

    // ========== PROJECT OPERATIONS ==========
    
    public static class Project {
        
        /**
         * Pure bytes-to-bytes projection operation that avoids all object creation.
         * Projects a subset of fields directly from a serialized Imprint record.
         */
        public static ByteBuffer projectBytes(ByteBuffer sourceBuffer, int... fieldIds) throws ImprintException {
            // Convert to ImprintBuffer for high-performance processing
            var source = ImprintBuffer.fromByteBuffer(sourceBuffer);
            
            // Use high-performance implementation
            var result = projectBytes(source, fieldIds);
            
            // Convert back to ByteBuffer for API compatibility
            return result.toByteBuffer().asReadOnlyBuffer();
        }

        public static ImprintBuffer projectBytes(ImprintBuffer sourceBuffer, int... fieldIds) throws ImprintException {
            Core.validateImprintBuffer(sourceBuffer, "sourceBuffer");

            if (fieldIds == null || fieldIds.length == 0) {
                return Core.createEmptyRecordBytes();
            }

            var sortedFieldIds = fieldIds.clone();
            Arrays.sort(sortedFieldIds);

            // Parse header directly from ImprintBuffer (faster than ByteBuffer)
            var header = Core.parseHeaderFromImprintBuffer(sourceBuffer);

            // Extract sections using ImprintBuffer operations
            var sections = Core.extractSectionsFromImprintBuffer(sourceBuffer, header);

            // Perform high-performance projection
            return projectRawSections(header, sections, sortedFieldIds);
        }

        private static ImprintBuffer projectRawSections(Header originalHeader, Core.ImprintBufferSections sections, int[] sortedRequestedFields) {
            
            if (sortedRequestedFields.length == 0) {
                return Core.createEmptyRecordBytes();
            }

            // Estimate reasonable initial size
            int maxFields = sortedRequestedFields.length;
            int estimatedDirectorySize = ImprintRecord.calculateDirectorySize(maxFields);
            int estimatedPayloadSize = sections.payloadBuffer.remaining(); // Conservative estimate
            int estimatedTotalSize = Constants.HEADER_BYTES + estimatedDirectorySize + estimatedPayloadSize;

            // Create growable buffer
            var output = ImprintBuffer.growable(estimatedTotalSize);

            // Reserve space for header and directory - write payload first
            int headerAndDirSize = Constants.HEADER_BYTES + estimatedDirectorySize;
            output.position(headerAndDirSize);

            // Project fields directly into growable buffer
            var projectedEntries = new ArrayList<Core.RawDirectoryEntry>(maxFields);
            int actualPayloadSize = projectFieldsDirectToBuffer(output, sections, sortedRequestedFields, projectedEntries);

            // Build and finalize the buffer
            return Core.buildFinalBuffer(originalHeader, projectedEntries, output,
                                                 estimatedDirectorySize, actualPayloadSize, headerAndDirSize);
        }

        private static int projectFieldsDirectToBuffer(ImprintBuffer output, Core.ImprintBufferSections sections, int[] sortedRequestedFields, List<Core.RawDirectoryEntry> projectedEntries) {
            
            var dirIterator = new Core.ImprintBufferDirectoryIterator(sections.directoryBuffer);
            
            int currentOffset = 0;
            int initialPosition = output.position();
            int requestedIndex = 0;

            // Merge algorithm: two-pointer approach through sorted sequences
            var currentEntry = dirIterator.hasNext() ? dirIterator.next() : null;

            while (currentEntry != null && requestedIndex < sortedRequestedFields.length) {
                int fieldId = currentEntry.fieldId;
                int targetFieldId = sortedRequestedFields[requestedIndex];

                if (fieldId == targetFieldId) {
                    // High-performance field copying using ImprintBuffer-to-ImprintBuffer transfer
                    int fieldSize = Core.getFieldSizeOptimized(sections.payloadBuffer, currentEntry.offset, dirIterator);
                    Core.copyField(output, sections.payloadBuffer, currentEntry.offset, fieldSize);

                    projectedEntries.add(new Core.RawDirectoryEntry(currentEntry.fieldId, currentEntry.typeCode, currentOffset));
                    currentOffset += fieldSize;

                    // Advance both pointers - handle dupes by advancing to next unique field
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

            return output.position() - initialPosition;
        }
    }
}