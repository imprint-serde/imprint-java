package com.imprint.types;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueTest {

    @Test
    void shouldCreateNullValue() {
        Value value = Value.nullValue();

        assertThat(value).isInstanceOf(Value.NullValue.class);
        assertThat(value.getTypeCode()).isEqualTo(TypeCode.NULL);
        assertThat(value.toString()).isEqualTo("null");
    }

    @Test
    void shouldCreateBooleanValues() {
        Value trueValue = Value.fromBoolean(true);
        Value falseValue = Value.fromBoolean(false);

        assertThat(trueValue).isInstanceOf(Value.BoolValue.class);
        assertThat(((Value.BoolValue) trueValue).getValue()).isTrue();
        assertThat(trueValue.getTypeCode()).isEqualTo(TypeCode.BOOL);

        assertThat(falseValue).isInstanceOf(Value.BoolValue.class);
        assertThat(((Value.BoolValue) falseValue).getValue()).isFalse();
        assertThat(falseValue.getTypeCode()).isEqualTo(TypeCode.BOOL);
    }

    @Test
    void shouldCreateNumericValues() {
        var int32 = Value.fromInt32(42);
        var int64 = Value.fromInt64(123456789L);
        var float32 = Value.fromFloat32(3.14f);
        var float64 = Value.fromFloat64(2.718281828);

        assertThat(int32.getTypeCode()).isEqualTo(TypeCode.INT32);
        assertThat(((Value.Int32Value) int32).getValue()).isEqualTo(42);

        assertThat(int64.getTypeCode()).isEqualTo(TypeCode.INT64);
        assertThat(((Value.Int64Value) int64).getValue()).isEqualTo(123456789L);

        assertThat(float32.getTypeCode()).isEqualTo(TypeCode.FLOAT32);
        assertThat(((Value.Float32Value) float32).getValue()).isEqualTo(3.14f);

        assertThat(float64.getTypeCode()).isEqualTo(TypeCode.FLOAT64);
        assertThat(((Value.Float64Value) float64).getValue()).isEqualTo(2.718281828);
    }

    @Test
    void shouldCreateBytesAndStringValues() {
        byte[] bytes = {1, 2, 3, 4};
        var bytesValue = Value.fromBytes(bytes);
        var stringValue = Value.fromString("hello");

        assertThat(bytesValue.getTypeCode()).isEqualTo(TypeCode.BYTES);
        assertThat(((Value.BytesValue) bytesValue).getValue()).isEqualTo(bytes);

        assertThat(stringValue.getTypeCode()).isEqualTo(TypeCode.STRING);
        assertThat(((Value.StringValue) stringValue).getValue()).isEqualTo("hello");
    }

    @Test
    void shouldCreateArrayValues() {
        List<Value> elements = Arrays.asList(
                Value.fromInt32(1),
                Value.fromInt32(2),
                Value.fromInt32(3)
        );
        Value arrayValue = Value.fromArray(elements);

        assertThat(arrayValue.getTypeCode()).isEqualTo(TypeCode.ARRAY);
        assertThat(((Value.ArrayValue) arrayValue).getValue()).isEqualTo(elements);
    }

    @Test
    void shouldCreateMapValues() {
        var map = new HashMap<MapKey, Value>();
        map.put(MapKey.fromString("key1"), Value.fromInt32(1));
        map.put(MapKey.fromString("key2"), Value.fromInt32(2));

        Value mapValue = Value.fromMap(map);

        assertThat(mapValue.getTypeCode()).isEqualTo(TypeCode.MAP);
        assertThat(((Value.MapValue) mapValue).getValue()).isEqualTo(map);
    }

    @Test
    void shouldHandleEqualityCorrectly() {
        var int1 = Value.fromInt32(42);
        var int2 = Value.fromInt32(42);
        var int3 = Value.fromInt32(43);

        assertThat(int1).isEqualTo(int2);
        assertThat(int1).isNotEqualTo(int3);
        assertThat(int1.hashCode()).isEqualTo(int2.hashCode());
    }

    @Test
    void shouldRejectNullString() {
        assertThatThrownBy(() -> Value.fromString(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCreateStringBufferValue() {
        String testString = "hello world";
        byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(utf8Bytes);

        Value stringBufferValue = Value.fromStringBuffer(buffer);

        assertThat(stringBufferValue).isInstanceOf(Value.StringBufferValue.class);
        assertThat(stringBufferValue.getTypeCode()).isEqualTo(TypeCode.STRING);
        assertThat(((Value.StringBufferValue) stringBufferValue).getValue()).isEqualTo(testString);
    }

    @Test
    void shouldCreateBytesBufferValue() {
        byte[] testBytes = {1, 2, 3, 4, 5};
        ByteBuffer buffer = ByteBuffer.wrap(testBytes);

        Value bytesBufferValue = Value.fromBytesBuffer(buffer);

        assertThat(bytesBufferValue).isInstanceOf(Value.BytesBufferValue.class);
        assertThat(bytesBufferValue.getTypeCode()).isEqualTo(TypeCode.BYTES);
        assertThat(((Value.BytesBufferValue) bytesBufferValue).getValue()).isEqualTo(testBytes);
    }

    @Test
    void shouldHandleStringBufferValueFastPath() {
        // Array-backed buffer with arrayOffset() == 0 should use fast path
        String testString = "fast path test";
        byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(utf8Bytes);

        Value stringBufferValue = Value.fromStringBuffer(buffer);

        // Should work correctly regardless of path taken
        assertThat(((Value.StringBufferValue) stringBufferValue).getValue()).isEqualTo(testString);
    }

    @Test
    void shouldHandleStringBufferValueFallbackPath() {
        // Sliced buffer will have non-zero arrayOffset, forcing fallback path
        String testString = "fallback path test";
        byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(utf8Bytes);
        ByteBuffer sliced = buffer.slice(); // This may break arrayOffset() == 0

        Value stringBufferValue = Value.fromStringBuffer(sliced);

        // Should work correctly regardless of path taken
        assertThat(((Value.StringBufferValue) stringBufferValue).getValue()).isEqualTo(testString);
    }

    @Test
    void shouldHandleLargeStringWithoutCaching() {
        // Create string > 1KB to test the no-cache path
        String largeString = "x".repeat(2000);
        byte[] utf8Bytes = largeString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(utf8Bytes).slice(); // Force fallback path

        Value stringBufferValue = Value.fromStringBuffer(buffer);

        assertThat(((Value.StringBufferValue) stringBufferValue).getValue()).isEqualTo(largeString);
    }

    @Test
    void shouldCacheStringDecoding() {
        String testString = "cache test";
        byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(utf8Bytes);

        Value.StringBufferValue stringBufferValue = (Value.StringBufferValue) Value.fromStringBuffer(buffer);

        // First call should decode and cache
        String result1 = stringBufferValue.getValue();
        // Second call should return cached value
        String result2 = stringBufferValue.getValue();

        assertThat(result1).isEqualTo(testString);
        assertThat(result2).isEqualTo(testString);
        assertThat(result1).isSameAs(result2); // Should be same object reference due to caching
    }

    @Test
    void shouldHandleStringValueEquality() {
        String testString = "equality test";

        Value stringValue = Value.fromString(testString);
        Value stringBufferValue = Value.fromStringBuffer(ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8)));

        assertThat(stringValue).isEqualTo(stringBufferValue);
        assertThat(stringBufferValue).isEqualTo(stringValue);
        assertThat(stringValue.hashCode()).isEqualTo(stringBufferValue.hashCode());
    }

    @Test
    void shouldHandleBytesValueEquality() {
        byte[] testBytes = {1, 2, 3, 4, 5};

        Value bytesValue = Value.fromBytes(testBytes);
        Value bytesBufferValue = Value.fromBytesBuffer(ByteBuffer.wrap(testBytes));

        assertThat(bytesValue).isEqualTo(bytesBufferValue);
        assertThat(bytesBufferValue).isEqualTo(bytesValue);
    }
}