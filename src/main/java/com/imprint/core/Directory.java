package com.imprint.core;

import com.imprint.types.TypeCode;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;

import java.util.List;
import java.util.Objects;

/**
 * Represents the common interface for a directory entry in an Imprint record.
 * A directory entry provides metadata about a field, such as its ID, type, and location in the payload.
 */
public interface Directory {
    /**
     * @return The field's unique identifier.
     */
    short getId();

    /**
     * @return The {@link TypeCode} of the field's value.
     */
    TypeCode getTypeCode();

    /**
     * @return The starting position (offset) of the field's data within the payload buffer.
     */
    int getOffset();

    /**
     * A view interface for accessing directory entries efficiently.
     * Provides both access to individual entries and full directory materialization.
     */
    interface DirectoryView {
        /**
         * Find a directory entry by field ID.
         * @param fieldId The field ID to search for
         * @return The directory entry if found, null otherwise
         */
        Directory findEntry(int fieldId);

        /**
         * Get all directory entries as a list, with full eager deserialization if necessary.
         * @return List of all directory entries in field ID order
         */
        List<Directory> toList();

        /**
         * Get the count of directory entries without parsing all entries.
         * @return Number of entries in the directory
         */
        int size();

        /**
         * Create an iterator for lazy directory traversal.
         * For buffer-backed views, this avoids parsing the entire directory upfront.
         * @return Iterator over directory entries in field ID order
         */
        java.util.Iterator<Directory> iterator();
    }

    /**
     * Immutable representation of the Imprint Directory used for deserialization,
     * merging, and field projections
     */
    @Value
    class Entry implements Directory {
        short id;
        TypeCode typeCode;
        int offset;

        public Entry(short id, TypeCode typeCode, int offset) {
            this.id = id;
            this.typeCode = Objects.requireNonNull(typeCode, "TypeCode cannot be null");
            this.offset = offset;
        }
    }
}