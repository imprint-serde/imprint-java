package com.imprint.core;

import com.imprint.types.TypeCode;
import lombok.Value;

import java.util.Objects;

/**
 * A concrete, immutable directory entry.
 */
@Value
public class SimpleDirectoryEntry implements DirectoryEntry {
    short id;
    TypeCode typeCode;
    int offset;

    public SimpleDirectoryEntry(short id, TypeCode typeCode, int offset) {
        this.id = id;
        this.typeCode = Objects.requireNonNull(typeCode, "TypeCode cannot be null");
        this.offset = offset;
    }
} 