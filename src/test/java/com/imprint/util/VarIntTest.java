package com.imprint.util;

import com.imprint.error.ImprintException;
import com.imprint.error.ErrorType;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.*;

class VarIntTest {
    
    @Test
    void shouldRoundtripCommonValues() throws ImprintException {
        int[] testCases = {
            0, 1, 127, 128, 16383, 16384, 2097151, 2097152,
            268435455, 268435456, -1 // -1 as unsigned is 0xFFFFFFFF
        };
        
        for (int value : testCases) {
            var buffer = new ImprintBuffer(new byte[10]);
            VarInt.encode(value, buffer);
            int encodedLength = buffer.position();
            
            buffer.flip();
            VarInt.DecodeResult result = VarInt.decode(buffer);
            
            assertThat(result.getValue()).isEqualTo(value);
            assertThat(result.getBytesRead()).isEqualTo(encodedLength);
        }
    }
    
    @Test
    void shouldEncodeKnownValuesCorrectly() {
        // Test cases with known encodings
        assertEncodedBytes(0, 0x00);
        assertEncodedBytes(1, 0x01);
        assertEncodedBytes(127, 0x7f);
        assertEncodedBytes(128, 0x80, 0x01);
        assertEncodedBytes(16383, 0xff, 0x7f);
        assertEncodedBytes(16384, 0x80, 0x80, 0x01);
    }
    
    private void assertEncodedBytes(int value, int... expectedBytes) {
        var buffer = new ImprintBuffer(new byte[10]);
        VarInt.encode(value, buffer);
        buffer.flip();
        
        byte[] actual = new byte[buffer.remaining()];
        buffer.get(actual);
        
        byte[] expected = new byte[expectedBytes.length];
        for (int i = 0; i < expectedBytes.length; i++) {
            expected[i] = (byte) expectedBytes[i];
        }
        
        assertThat(actual).containsExactly(expected);
    }
    
    @Test
    void shouldWorkWithByteBuffer() throws ImprintException {
        var buffer = new ImprintBuffer(new byte[10]);
        VarInt.encode(16384, buffer);
        
        buffer.flip();
        VarInt.DecodeResult result = VarInt.decode(buffer);
        
        assertThat(result.getValue()).isEqualTo(16384);
        assertThat(result.getBytesRead()).isEqualTo(3);
    }
    
    @Test
    void shouldCalculateEncodedLength() {
        assertThat(VarInt.encodedLength(0)).isEqualTo(1);
        assertThat(VarInt.encodedLength(127)).isEqualTo(1);
        assertThat(VarInt.encodedLength(128)).isEqualTo(2);
        assertThat(VarInt.encodedLength(16383)).isEqualTo(2);
        assertThat(VarInt.encodedLength(16384)).isEqualTo(3);
        assertThat(VarInt.encodedLength(-1)).isEqualTo(5); // max u32
    }
    
    @Test
    void shouldHandleBufferUnderflow() {
        var buffer = new ImprintBuffer(new byte[1]);
        buffer.putByte((byte) 0x80); // incomplete varint
        buffer.flip();
        
        assertThatThrownBy(() -> VarInt.decode(buffer))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.BUFFER_UNDERFLOW);
    }
    
    @Test
    void shouldHandleOverlongEncoding() {
        var buffer = new ImprintBuffer(new byte[10]);
        buffer.putBytes(new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01});
        buffer.flip();
        
        assertThatThrownBy(() -> VarInt.decode(buffer))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.MALFORMED_VARINT);
    }
    
    @Test
    void shouldHandleOverflow() {
        var buffer = new ImprintBuffer(new byte[10]);
        buffer.putBytes(new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10});
        buffer.flip();
        
        assertThatThrownBy(() -> VarInt.decode(buffer))
            .isInstanceOf(ImprintException.class)
            .extracting("errorType")
            .isEqualTo(ErrorType.MALFORMED_VARINT);
    }
}