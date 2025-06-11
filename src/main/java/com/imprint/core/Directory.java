package com.imprint.core;

import com.imprint.types.TypeCode;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;

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

    /**
     * Mutable representation of the Imprint Directory bound with corresponding type value
     * used for record building through {@link ImprintRecordBuilder} and subsequent serialization.
     */
    @Getter
    class Builder implements Directory {
        private final short id;
        private final com.imprint.types.Value value;
        @Setter
        private int offset;

        Builder(short id, com.imprint.types.Value value) {
            this.id = id;
            this.value = value;
            this.offset = -1;
        }

        @Override
        public TypeCode getTypeCode() {
            return value.getTypeCode();
        }
    }
}