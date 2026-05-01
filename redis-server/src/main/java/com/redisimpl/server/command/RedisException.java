package com.redisimpl.server.command;

/**
 * Exception representing a Redis error response.
 * The message is sent directly as a RESP error to the client.
 */
public class RedisException extends RuntimeException {

    public RedisException(String message) {
        super(message);
    }

    // Common error messages (matching official Redis exactly)
    public static final String ERR_WRONG_TYPE =
            "WRONGTYPE Operation against a key holding the wrong kind of value";
    public static final String ERR_NOT_INTEGER =
            "ERR value is not an integer or out of range";
    public static final String ERR_NAN_OR_INF =
            "ERR increment would produce NaN or Infinity";
    public static final String ERR_NO_SUCH_KEY =
            "ERR no such key";
    public static final String ERR_SYNTAX =
            "ERR syntax error";
    public static final String ERR_OUT_OF_RANGE =
            "ERR value is out of range, must be positive";
    public static final String ERR_DB_INDEX =
            "ERR DB index is out of range";

    public static RedisException wrongType() {
        return new RedisException(ERR_WRONG_TYPE);
    }

    public static RedisException notInteger() {
        return new RedisException(ERR_NOT_INTEGER);
    }

    public static RedisException nanOrInf() {
        return new RedisException(ERR_NAN_OR_INF);
    }

    public static RedisException syntax() {
        return new RedisException(ERR_SYNTAX);
    }

    public static RedisException outOfRange() {
        return new RedisException(ERR_OUT_OF_RANGE);
    }

    public static RedisException dbIndex() {
        return new RedisException(ERR_DB_INDEX);
    }
}
