package com.imprint.core;

import com.imprint.types.TypeCode;
import lombok.Value;

import java.util.Objects;

/**
 * A directory entry describing a single field in an Imprint record.
 * Each entry has a fixed size of 7 bytes.
 */
@Value
public class DirectoryEntry {
    short id;
    TypeCode typeCode;
    int offset;
    
    public DirectoryEntry(int id, TypeCode typeCode, int offset) {
        this.id = (short) id;
        this.typeCode = Objects.requireNonNull(typeCode, "TypeCode cannot be null");
        this.offset = offset;
    }
}