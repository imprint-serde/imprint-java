package com.imprint.types;

import org.junit.jupiter.api.Test;

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
    void shouldDefensiveCopyArrays() {
        byte[] original = {1, 2, 3};
        var bytesValue = Value.fromBytes(original);
        
        // Modify original array
        original[0] = 99;
        
        // Value should be unchanged
        assertThat(((Value.BytesValue) bytesValue).getValue()).containsExactly(1, 2, 3);
    }
    
    @Test
    void shouldRejectNullString() {
        assertThatThrownBy(() -> Value.fromString(null))
            .isInstanceOf(NullPointerException.class);
    }
}