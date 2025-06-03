package com.imprint.core;

import lombok.Value;

/**
 * The header of an Imprint record.
 */
@Value
public class Header {
    Flags flags;
    SchemaId schemaId;
    int payloadSize;
}