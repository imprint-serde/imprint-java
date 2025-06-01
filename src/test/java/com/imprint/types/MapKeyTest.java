package com.imprint.types;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MapKeyTest {
    
    @Test
    void shouldCreateMapKeysFromValues() throws ImprintException {
        var int32Key = MapKey.fromValue(Value.fromInt32(42));
        var int64Key = MapKey.fromValue(Value.fromInt64(123L));
        var bytesKey = MapKey.fromValue(Value.fromBytes(new byte[]{1, 2, 3}));
        var stringKey = MapKey.fromValue(Value.fromString("test"));
        
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
    void shouldConvertBackToValues() {
        var int32Key = MapKey.fromInt32(42);
        var stringKey = MapKey.fromString("test");

        var int32Value = int32Key.toValue();
        var stringValue = stringKey.toValue();
        
        assertThat(int32Value).isInstanceOf(Value.Int32Value.class);
        assertThat(((Value.Int32Value) int32Value).getValue()).isEqualTo(42);
        
        assertThat(stringValue).isInstanceOf(Value.StringValue.class);
        assertThat(((Value.StringValue) stringValue).getValue()).isEqualTo("test");
    }
    
    @Test
    void shouldRejectInvalidValueTypes() {
        var boolValue = Value.fromBoolean(true);
        var arrayValue = Value.fromArray(java.util.Collections.emptyList());
        
        assertThatThrownBy(() -> MapKey.fromValue(boolValue))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.TYPE_MISMATCH);
            
        assertThatThrownBy(() -> MapKey.fromValue(arrayValue))
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