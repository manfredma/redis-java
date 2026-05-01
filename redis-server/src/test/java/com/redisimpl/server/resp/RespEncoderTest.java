package com.redisimpl.server.resp;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespEncoderTest {

    private static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void encodeSimpleString() {
        byte[] encoded = RespEncoder.encodeSimpleString("OK");
        assertEquals("+OK\r\n", str(encoded));
    }

    @Test
    void encodeError() {
        byte[] encoded = RespEncoder.encodeError("ERR unknown command");
        assertEquals("-ERR unknown command\r\n", str(encoded));
    }

    @Test
    void encodeInteger_positive() {
        byte[] encoded = RespEncoder.encodeInteger(42);
        assertEquals(":42\r\n", str(encoded));
    }

    @Test
    void encodeInteger_negative() {
        byte[] encoded = RespEncoder.encodeInteger(-1);
        assertEquals(":-1\r\n", str(encoded));
    }

    @Test
    void encodeInteger_zero() {
        byte[] encoded = RespEncoder.encodeInteger(0);
        assertEquals(":0\r\n", str(encoded));
    }

    @Test
    void encodeBulkString_normal() {
        byte[] encoded = RespEncoder.encodeBulkString("hello".getBytes(StandardCharsets.UTF_8));
        assertEquals("$5\r\nhello\r\n", str(encoded));
    }

    @Test
    void encodeBulkString_null() {
        byte[] encoded = RespEncoder.encodeBulkString(null);
        assertEquals("$-1\r\n", str(encoded));
    }

    @Test
    void encodeBulkString_empty() {
        byte[] encoded = RespEncoder.encodeBulkString(new byte[0]);
        assertEquals("$0\r\n\r\n", str(encoded));
    }

    @Test
    void encodeArray_normal() {
        List<Object> items = Arrays.asList(
                "hello".getBytes(StandardCharsets.UTF_8),
                "world".getBytes(StandardCharsets.UTF_8)
        );
        byte[] encoded = RespEncoder.encodeArray(items);
        assertEquals("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", str(encoded));
    }

    @Test
    void encodeArray_null() {
        byte[] encoded = RespEncoder.encodeArray(null);
        assertEquals("*-1\r\n", str(encoded));
    }

    @Test
    void encodeArray_empty() {
        byte[] encoded = RespEncoder.encodeArray(Arrays.asList());
        assertEquals("*0\r\n", str(encoded));
    }

    @Test
    void encodeArray_withNull() {
        List<Object> items = Arrays.asList(
                "hello".getBytes(StandardCharsets.UTF_8),
                null
        );
        byte[] encoded = RespEncoder.encodeArray(items);
        assertEquals("*2\r\n$5\r\nhello\r\n$-1\r\n", str(encoded));
    }

    @Test
    void encodeNull_resp3() {
        byte[] encoded = RespEncoder.encodeNull();
        assertEquals("_\r\n", str(encoded));
    }

    @Test
    void encodeBoolean_true() {
        byte[] encoded = RespEncoder.encodeBoolean(true);
        assertEquals("#t\r\n", str(encoded));
    }

    @Test
    void encodeBoolean_false() {
        byte[] encoded = RespEncoder.encodeBoolean(false);
        assertEquals("#f\r\n", str(encoded));
    }

    @Test
    void encodeDouble_normal() {
        byte[] encoded = RespEncoder.encodeDouble(3.14);
        assertTrue(str(encoded).startsWith(",3.14"));
        assertTrue(str(encoded).endsWith("\r\n"));
    }

    @Test
    void encodeDouble_infinity() {
        byte[] encoded = RespEncoder.encodeDouble(Double.POSITIVE_INFINITY);
        assertEquals(",inf\r\n", str(encoded));
    }

    @Test
    void encodeDouble_negInfinity() {
        byte[] encoded = RespEncoder.encodeDouble(Double.NEGATIVE_INFINITY);
        assertEquals(",-inf\r\n", str(encoded));
    }
}
