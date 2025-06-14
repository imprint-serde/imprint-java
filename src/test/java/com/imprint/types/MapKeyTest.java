package com.imprint.types;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MapKeyTest {
    
    @Test
    void shouldCreateMapKeysFromPrimitives() throws ImprintException {
        var int32Key = MapKey.fromPrimitive(TypeCode.INT32, 42);
        var int64Key = MapKey.fromPrimitive(TypeCode.INT64, 123L);
        var bytesKey = MapKey.fromPrimitive(TypeCode.BYTES, new byte[]{1, 2, 3});
        var stringKey = MapKey.fromPrimitive(TypeCode.STRING, "test");
        
        assertThat(int32Key).isInstanceOf(MapKey.Int32Key.class);
        assertThat(((MapKey.Int32Key) int32Key).getValue()).isEqualTo(42);
        
        assertThat(int64Key).isInstanceOf(MapKey.Int64Key.class);
        assertThat(((MapKey.Int64Key) int64Key).getValue()).isEqualTo(123L);
        
        assertThat(bytesKey).isInstanceOf(MapKey.BytesKey.class);
        assertThat(((MapKey.BytesKey) bytesKey).getValue()).containsExactly(1, 2, 3);
        
        assertThat(stringKey).isInstanceOf(MapKey.StringKey.class);
        assertThat(((MapKey.StringKey) stringKey).getValue()).isEqualTo("test");
    }
    
    @Test
    void shouldConvertToPrimitives() {
        var int32Key = MapKey.fromInt32(42);
        var stringKey = MapKey.fromString("test");

        Object int32Value = int32Key.getPrimitiveValue();
        Object stringValue = stringKey.getPrimitiveValue();
        
        assertThat(int32Value).isInstanceOf(Integer.class);
        assertThat(int32Value).isEqualTo(42);
        
        assertThat(stringValue).isInstanceOf(String.class);
        assertThat(stringValue).isEqualTo("test");
    }
    
    @Test
    void shouldRejectInvalidPrimitiveTypes() {
        assertThatThrownBy(() -> MapKey.fromPrimitive(TypeCode.BOOL, true))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.TYPE_MISMATCH);
            
        assertThatThrownBy(() -> MapKey.fromPrimitive(TypeCode.ARRAY, java.util.Collections.emptyList()))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.TYPE_MISMATCH);
    }
    
    @Test
    void shouldHandleEqualityAndHashing() {
        var key1 = MapKey.fromString("test");
        var key2 = MapKey.fromString("test");
        var key3 = MapKey.fromString("different");
        
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotEqualTo(key3);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }
    
    @Test
    void shouldDefensiveCopyBytes() {
        byte[] original = {1, 2, 3};
        var bytesKey = MapKey.fromBytes(original);
        
        // Modify original array
        original[0] = 99;
        
        // Key should be unchanged
        assertThat(((MapKey.BytesKey) bytesKey).getValue()).containsExactly(1, 2, 3);
    }
    
    @Test
    void shouldHaveCorrectTypeCodes() {
        assertThat(MapKey.fromInt32(1).getTypeCode()).isEqualTo(TypeCode.INT32);
        assertThat(MapKey.fromInt64(1L).getTypeCode()).isEqualTo(TypeCode.INT64);
        assertThat(MapKey.fromBytes(new byte[]{1}).getTypeCode()).isEqualTo(TypeCode.BYTES);
        assertThat(MapKey.fromString("test").getTypeCode()).isEqualTo(TypeCode.STRING);
    }
}