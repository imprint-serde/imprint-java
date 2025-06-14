package com.imprint.core;

import com.imprint.Constants;
import lombok.Value;

import java.nio.ByteBuffer;

/**
 * The header of an Imprint record.
 */
@Value
public class Header {
    Flags flags;
    SchemaId schemaId;
    int payloadSize;
    
    /**
     * Serialize this header to a ByteBuffer.
     * Follows the Imprint header format: magic(1) + version(1) + flags(1) + fieldSpaceId(4) + schemaHash(4) + payloadSize(4).
     */
    public void serialize(ByteBuffer buffer) {
        buffer.put(Constants.MAGIC);
        buffer.put(Constants.VERSION);
        buffer.put(flags.getValue());
        buffer.putInt(schemaId.getFieldSpaceId());
        buffer.putInt(schemaId.getSchemaHash());
        buffer.putInt(payloadSize);
    }
    
    /**
     * Static helper for serializing any header to a ByteBuffer.
     */
    public static void serialize(Header header, ByteBuffer buffer) {
        header.serialize(buffer);
    }
}