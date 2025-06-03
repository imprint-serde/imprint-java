package com.imprint.core;

import lombok.Value;

/**
 * Bit flags for Imprint record header.
 * Currently reserved for future use - field directory is always present.
 */
@Value
public class Flags {
    byte value;
}