package com.redisimpl.server.resp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * RESP2/3 encoder.
 * Encodes Java values into RESP wire format bytes.
 */
public final class RespEncoder {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    private RespEncoder() {}

    // ---- RESP2 types ----

    /** Simple String: +OK\r\n */
    public static byte[] encodeSimpleString(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Error: -ERR message\r\n */
    public static byte[] encodeError(String msg) {
        return ("-" + msg + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Integer: :42\r\n */
    public static byte[] encodeInteger(long n) {
        return (":" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Bulk String: $5\r\nhello\r\n or $-1\r\n for null */
    public static byte[] encodeBulkString(byte[] data) {
        if (data == null) return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.write(data);
            out.write(CRLF);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    /**
     * Array: *N\r\n + encoded elements, or *-1\r\n for null.
     * Each element should be byte[] (encoded as bulk string) or null.
     */
    public static byte[] encodeArray(List<Object> items) {
        if (items == null) return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (Object item : items) {
                if (item == null) {
                    out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
                } else if (item instanceof byte[]) {
                    out.write(encodeBulkString((byte[]) item));
                } else if (item instanceof String) {
                    out.write(encodeBulkString(((String) item).getBytes(StandardCharsets.UTF_8)));
                } else if (item instanceof Long || item instanceof Integer) {
                    out.write(encodeInteger(((Number) item).longValue()));
                } else if (item instanceof List) {
                    // Nested array — encode recursively
                    @SuppressWarnings("unchecked")
                    List<Object> nested = (List<Object>) item;
                    out.write(encodeArray(nested));
                } else {
                    out.write(encodeBulkString(item.toString().getBytes(StandardCharsets.UTF_8)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- RESP3 types ----

    /** Null: _\r\n */
    public static byte[] encodeNull() {
        return "_\r\n".getBytes(StandardCharsets.UTF_8);
    }

    /** Boolean: #t\r\n or #f\r\n */
    public static byte[] encodeBoolean(boolean b) {
        return (b ? "#t\r\n" : "#f\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Double: ,3.14\r\n or ,inf\r\n or ,-inf\r\n */
    public static byte[] encodeDouble(double d) {
        String s;
        if (Double.isInfinite(d)) {
            s = d > 0 ? "inf" : "-inf";
        } else if (Double.isNaN(d)) {
            s = "nan";
        } else {
            // Format without trailing zeros
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                s = String.valueOf((long) d);
            } else {
                s = String.valueOf(d);
            }
        }
        return ("," + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Big Number: (12345\r\n */
    public static byte[] encodeBigNumber(String n) {
        return ("(" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Encode a status/OK response.
     */
    public static final byte[] OK = encodeSimpleString("OK");
    public static final byte[] PONG = encodeSimpleString("PONG");
    public static final byte[] EMPTY_ARRAY = encodeArray(java.util.Collections.emptyList());
    public static final byte[] NULL_BULK = encodeBulkString(null);
    public static final byte[] ZERO = encodeInteger(0);
    public static final byte[] ONE = encodeInteger(1);
}
