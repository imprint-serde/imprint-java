package com.imprint.core;

import lombok.Value;

/**
 * Schema identifier containing field-space ID and schema hash.
 */
@Value
public class SchemaId {
    int fieldspaceId;
    int schemaHash;
}