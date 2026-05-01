package com.redisimpl.test.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Lua scripting (EVAL/EVALSHA/SCRIPT) integration tests")
class ScriptIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("EVAL returns integer result")
    void eval_integer() {
        Object result = jedis.eval("return 42", 0);
        assertEquals(42L, result);
    }

    @Test
    @DisplayName("EVAL returns string result")
    void eval_string() {
        Object result = jedis.eval("return 'hello'", 0);
        assertEquals("hello", result);
    }

    @Test
    @DisplayName("EVAL accesses KEYS and ARGV")
    void eval_keysAndArgv() {
        jedis.set("mykey", "myvalue");
        Object result = jedis.eval(
            "return redis.call('get', KEYS[1])",
            Collections.singletonList("mykey"),
            Collections.emptyList());
        assertEquals("myvalue", result);
    }

    @Test
    @DisplayName("EVAL with redis.call SET and GET")
    void eval_setAndGet() {
        jedis.eval(
            "redis.call('set', KEYS[1], ARGV[1])",
            Collections.singletonList("luakey"),
            Collections.singletonList("luavalue"));
        assertEquals("luavalue", jedis.get("luakey"));
    }

    @Test
    @DisplayName("SCRIPT LOAD returns SHA1 hex string")
    void scriptLoad() {
        String script = "return 'loaded'";
        String sha = jedis.scriptLoad(script);
        assertNotNull(sha);
        assertEquals(40, sha.length()); // SHA1 is 40 hex chars
        assertTrue(sha.matches("[0-9a-f]{40}"));
    }

    @Test
    @DisplayName("EVALSHA executes cached script")
    void evalsha() {
        String script = "return 'cached'";
        String sha = jedis.scriptLoad(script);
        Object result = jedis.evalsha(sha, 0);
        assertEquals("cached", result);
    }

    @Test
    @DisplayName("SCRIPT FLUSH removes all scripts")
    void scriptFlush() {
        String sha = jedis.scriptLoad("return 1");
        jedis.scriptFlush();
        // After flush, evalsha should fail
        try {
            jedis.evalsha(sha, 0);
            fail("Expected NOSCRIPT error");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("NOSCRIPT") || e.getMessage().contains("noscript"),
                "Expected NOSCRIPT but got: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("EVALSHA with unknown SHA returns NOSCRIPT error")
    void evalsha_noscript() {
        try {
            jedis.evalsha("0000000000000000000000000000000000000000", 0);
            fail("Expected exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("NOSCRIPT"),
                "Expected NOSCRIPT but got: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("EVAL with boolean true returns 1")
    void eval_boolean_true() {
        Object result = jedis.eval("return true", 0);
        assertEquals(1L, result);
    }

    @Test
    @DisplayName("EVAL with boolean false returns nil (null)")
    void eval_boolean_false() {
        Object result = jedis.eval("return false", 0);
        assertNull(result);
    }
}
