package com.imprint.core;

import com.imprint.types.TypeCode;

/**
 * Represents the common interface for a directory entry in an Imprint record.
 * A directory entry provides metadata about a field, such as its ID, type, and location in the payload.
 */
public interface DirectoryEntry {
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
}