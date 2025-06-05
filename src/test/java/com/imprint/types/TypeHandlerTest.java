package com.imprint.types;

import com.imprint.error.ImprintException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for individual TypeHandler implementations.
 * Validates serialization, deserialization, and size estimation for each type.
 */
class TypeHandlerTest {

    @Test
    void testNullHandler() throws ImprintException {
        var handler = TypeHandler.NULL;
        var value = Value.nullValue();

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(0);

        // Serialization
        var buffer = ByteBuffer.allocate(10);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(0); // NULL writes nothing

        // Deserialization
        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBoolHandler(boolean testValue) throws ImprintException {
        var handler = TypeHandler.BOOL;
        var value = Value.fromBoolean(testValue);

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(1);

        // Round-trip test
        var buffer = ByteBuffer.allocate(10);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(1);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);
        assertThat(((Value.BoolValue) deserialized).getValue()).isEqualTo(testValue);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 42, -42})
    void testInt32Handler(int testValue) throws ImprintException {
        var handler = TypeHandler.INT32;
        var value = Value.fromInt32(testValue);

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(4);

        // Round-trip test
        var buffer = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(4);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);
        assertThat(((Value.Int32Value) deserialized).getValue()).isEqualTo(testValue);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 123456789L})
    void testInt64Handler(long testValue) throws ImprintException {
        var handler = TypeHandler.INT64;
        var value = Value.fromInt64(testValue);

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(8);

        // Round-trip test
        var buffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(8);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);
        assertThat(((Value.Int64Value) deserialized).getValue()).isEqualTo(testValue);
    }

    @ParameterizedTest
    @ValueSource(floats = {0.0f, 1.0f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE, 3.14159f, Float.NaN, Float.POSITIVE_INFINITY})
    void testFloat32Handler(float testValue) throws ImprintException {
        var handler = TypeHandler.FLOAT32;
        var value = Value.fromFloat32(testValue);

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(4);

        // Round-trip test
        var buffer = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(4);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);

        float deserializedValue = ((Value.Float32Value) deserialized).getValue();
        if (Float.isNaN(testValue)) {
            assertThat(deserializedValue).isNaN();
        } else {
            assertThat(deserializedValue).isEqualTo(testValue);
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0, 1.0, -1.0, Double.MAX_VALUE, Double.MIN_VALUE, Math.PI, Double.NaN, Double.POSITIVE_INFINITY})
    void testFloat64Handler(double testValue) throws ImprintException {
        var handler = TypeHandler.FLOAT64;
        var value = Value.fromFloat64(testValue);

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(8);

        // Round-trip test
        var buffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);
        assertThat(buffer.position()).isEqualTo(8);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(value);

        double deserializedValue = ((Value.Float64Value) deserialized).getValue();
        if (Double.isNaN(testValue)) {
            assertThat(deserializedValue).isNaN();
        } else {
            assertThat(deserializedValue).isEqualTo(testValue);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "hello", "世界", "a very long string that exceeds typical buffer sizes and contains unicode: 🚀🎉", "null\0bytes"})
    void testStringHandler(String testValue) throws ImprintException {
        var handler = TypeHandler.STRING;
        var value = Value.fromString(testValue);

        byte[] utf8Bytes = testValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int expectedSize = com.imprint.util.VarInt.encodedLength(utf8Bytes.length) + utf8Bytes.length;

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(expectedSize);

        // Round-trip test
        var buffer = ByteBuffer.allocate(expectedSize + 20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);

        // Should return StringBufferValue (zero-copy implementation)
        assertThat(deserialized).isInstanceOf(Value.StringBufferValue.class);

        String deserializedString;
        if (deserialized instanceof Value.StringBufferValue) {
            deserializedString = ((Value.StringBufferValue) deserialized).getValue();
        } else {
            deserializedString = ((Value.StringValue) deserialized).getValue();
        }

        assertThat(deserializedString).isEqualTo(testValue);
    }

    @Test
    void testBytesHandlerWithArrayValue() throws ImprintException {
        var handler = TypeHandler.BYTES;
        byte[] testBytes = {0, 1, 2, (byte) 0xFF, 42, 127, -128};
        var value = Value.fromBytes(testBytes);

        int expectedSize = com.imprint.util.VarInt.encodedLength(testBytes.length) + testBytes.length;

        // Size estimation
        assertThat(handler.estimateSize(value)).isEqualTo(expectedSize);

        // Round-trip test
        var buffer = ByteBuffer.allocate(expectedSize + 20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(value, buffer);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);

        // Should return BytesBufferValue (zero-copy implementation)
        assertThat(deserialized).isInstanceOf(Value.BytesBufferValue.class);

        byte[] deserializedBytes = ((Value.BytesBufferValue) deserialized).getValue();
        assertThat(deserializedBytes).isEqualTo(testBytes);
    }

    @Test
    void testBytesHandlerWithBufferValue() throws ImprintException {
        var handler = TypeHandler.BYTES;
        byte[] testBytes = {10, 20, 30, 40};
        var bufferValue = Value.fromBytesBuffer(ByteBuffer.wrap(testBytes).asReadOnlyBuffer());

        int expectedSize = com.imprint.util.VarInt.encodedLength(testBytes.length) + testBytes.length;

        // Size estimation
        assertThat(handler.estimateSize(bufferValue)).isEqualTo(expectedSize);

        // Round-trip test
        var buffer = ByteBuffer.allocate(expectedSize + 20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(bufferValue, buffer);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);

        byte[] deserializedBytes = ((Value.BytesBufferValue) deserialized).getValue();
        assertThat(deserializedBytes).isEqualTo(testBytes);
    }

    @Test
    void testStringHandlerWithBufferValue() throws ImprintException {
        var handler = TypeHandler.STRING;
        String testString = "zero-copy string test";
        byte[] utf8Bytes = testString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var bufferValue = Value.fromStringBuffer(ByteBuffer.wrap(utf8Bytes).asReadOnlyBuffer());

        int expectedSize = com.imprint.util.VarInt.encodedLength(utf8Bytes.length) + utf8Bytes.length;

        // Size estimation
        assertThat(handler.estimateSize(bufferValue)).isEqualTo(expectedSize);

        // Round-trip test
        var buffer = ByteBuffer.allocate(expectedSize + 20).order(ByteOrder.LITTLE_ENDIAN);
        handler.serialize(bufferValue, buffer);

        buffer.flip();
        var deserialized = handler.deserialize(buffer);

        String deserializedString = ((Value.StringBufferValue) deserialized).getValue();
        assertThat(deserializedString).isEqualTo(testString);
    }

    @Test
    void testBoolHandlerInvalidValue() {
        var handler = TypeHandler.BOOL;
        var buffer = ByteBuffer.allocate(10);
        buffer.put((byte) 2); // Invalid boolean value
        buffer.flip();

        assertThatThrownBy(() -> handler.deserialize(buffer))
                .isInstanceOf(ImprintException.class)
                .hasMessageContaining("Invalid boolean value: 2");
    }

    @Test
    void testHandlerBufferUnderflow() {
        // Test that handlers properly detect buffer underflow
        var int32Handler = TypeHandler.INT32;
        var buffer = ByteBuffer.allocate(2); // Too small for int32

        assertThatThrownBy(() -> int32Handler.deserialize(buffer))
                .isInstanceOf(ImprintException.class)
                .hasMessageContaining("Not enough bytes for int32");
    }
}