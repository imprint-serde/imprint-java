package com.imprint.ops;

import com.imprint.core.*;
import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

@UtilityClass
public class ImprintOperations {

    /**
     * Project a subset of fields from an Imprint record. Payload copying is proportional to projected data size.
     *
     * <p><strong>Algorithm:</strong></p>
     * <ol>
     * <li>Sort and deduplicate requested field IDs for efficient matching</li>
     * <li>Scan directory to find matching fields and calculate ranges</li>
     * <li>Allocate new payload buffer with exact size needed</li>
     * <li>Copy field data ranges directly (zero-copy where possible)</li>
     * <li>Build new directory with adjusted offsets</li>
     * </ol>
     *
     * @param record The source record to project from
     * @param fieldIds Array of field IDs to include in projection
     * @return New ImprintRecord containing only the requested fields
     */
    public static ImprintRecord project(ImprintRecord record, int... fieldIds) {
        // Sort and deduplicate field IDs for efficient matching
        final var fieldIdSet = Arrays.stream(fieldIds)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));
        if (fieldIdSet.isEmpty()) {
            return createEmptyRecord(record.getHeader().getSchemaId());
        }

        var newDirectory = new ArrayList<Directory>(fieldIdSet.size());
        var payloadChunks = new ArrayList<ByteBuffer>(fieldIdSet.size());
        int currentOffset = 0;

        for (int fieldId : fieldIdSet) {
            // Use efficient lookup for each field's metadata. Returns null on failure.
            var sourceEntry = record.getDirectoryEntry(fieldId);

            // If field exists, get its payload and add to the new record components
            if (sourceEntry != null) {
                var fieldPayload = record.getRawBytes(sourceEntry);
                // This check is for internal consistency. If an entry exists, payload should too.
                if (fieldPayload != null) {
                    newDirectory.add(new Directory.Entry((short)fieldId, sourceEntry.getTypeCode(), currentOffset));
                    payloadChunks.add(fieldPayload);
                    currentOffset += fieldPayload.remaining();
                }
            }
        }

        // Build new payload from collected chunks
        ByteBuffer newPayload = buildPayloadFromChunks(payloadChunks, currentOffset);

        // Create new header with updated payload size
        // TODO: compute correct schema hash
        var newHeader = new Header(record.getHeader().getFlags(),
                new SchemaId(record.getHeader().getSchemaId().getFieldSpaceId(), 0xdeadbeef),
                newPayload.remaining()
        );

        return new ImprintRecord(newHeader, newDirectory, newPayload);
    }

    /**
     * Merge two Imprint records, combining their fields. Payload copying is proportional to total data size.
     *
     * <p><strong>Merge Strategy:</strong></p>
     * <ul>
     * <li>Fields are merged using sort-merge algorithm on directory entries</li>
     * <li>For duplicate field IDs: first record's field takes precedence</li>
     * <li>Payloads are concatenated with directory offsets adjusted</li>
     * <li>Schema ID from first record is preserved</li>
     * </ul>
     * </p>
     *
     * @param first The first record (takes precedence for duplicate fields)
     * @param second The second record to merge
     * @return New ImprintRecord containing merged fields
     * @throws ImprintException if merge fails due to incompatible records
     */
    public static ImprintRecord merge(ImprintRecord first, ImprintRecord second) throws ImprintException {
        var firstDir = first.getDirectory();
        var secondDir = second.getDirectory();

        // Pre-allocate for worst case (no overlapping fields)
        var newDirectory = new ArrayList<Directory>(firstDir.size() + secondDir.size());
        var payloadChunks = new ArrayList<ByteBuffer>();

        int firstIdx = 0;
        int secondIdx = 0;
        int currentOffset = 0;

        while (firstIdx < firstDir.size() || secondIdx < secondDir.size()) {
            Directory currentEntry;
            ByteBuffer currentPayload;

            if (firstIdx < firstDir.size() &&
                    (secondIdx >= secondDir.size() || firstDir.get(firstIdx).getId() <= secondDir.get(secondIdx).getId())) {

                // Take from first record
                currentEntry = firstDir.get(firstIdx);

                // Skip duplicate field in second record if present
                if (secondIdx < secondDir.size() &&
                        firstDir.get(firstIdx).getId() == secondDir.get(secondIdx).getId()) {
                    secondIdx++;
                }
                currentPayload = first.getRawBytes(currentEntry);
                firstIdx++;
            } else {
                // Take from second record
                currentEntry = secondDir.get(secondIdx);
                currentPayload = second.getRawBytes(currentEntry);
                secondIdx++;
            }

            if (currentPayload == null)
                throw new ImprintException(ErrorType.BUFFER_UNDERFLOW, "Failed to get raw bytes for field " + currentEntry.getId());

            // Add adjusted directory entry
            var newEntry = new Directory.Entry(currentEntry.getId(),
                    currentEntry.getTypeCode(), currentOffset);
            newDirectory.add(newEntry);

            // Collect payload chunk
            payloadChunks.add(currentPayload.duplicate());
            currentOffset += currentPayload.remaining();
        }

        // Build merged payload
        var mergedPayload = buildPayloadFromChunks(payloadChunks, currentOffset);

        // Create header preserving first record's schema ID
        var newHeader = new Header(first.getHeader().getFlags(),
                first.getHeader().getSchemaId(), mergedPayload.remaining());
        return new ImprintRecord(newHeader, newDirectory, mergedPayload);
    }

    /**
     * Build a new payload buffer by concatenating chunks.
     */
    private static ByteBuffer buildPayloadFromChunks(List<ByteBuffer> chunks, int totalSize) {
        var mergedPayload = ByteBuffer.allocate(totalSize);
        mergedPayload.order(ByteOrder.LITTLE_ENDIAN);
        for (var chunk : chunks)
            mergedPayload.put(chunk);
        mergedPayload.flip();
        return mergedPayload;
    }

    /**
     * Create an empty record with the given schema ID.
     */
    private static ImprintRecord createEmptyRecord(SchemaId schemaId) {
        var header = new Header(new Flags((byte) 0x01), schemaId, 0);
        return new ImprintRecord(header, Collections.emptyList(), ByteBuffer.allocate(0));
    }
}
