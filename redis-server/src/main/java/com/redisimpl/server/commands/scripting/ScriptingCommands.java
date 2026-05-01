package com.redisimpl.server.commands.scripting;

import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.resp.RespEncoder;
import org.luaj.vm2.*;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lua scripting command implementations: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH.
 */
public final class ScriptingCommands {

    private final RedisServer server;
    /** SHA1 -> script source cache */
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

    public ScriptingCommands(RedisServer server) {
        this.server = server;
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- EVAL ----

    @RedisCommand(name = "eval", arity = -3, flags = "noscript", firstKey = 0, lastKey = 0, step = 0)
    public byte[] eval(RedisClient client, byte[][] argv) {
        String script = toStr(argv[1]);
        int numkeys;
        try {
            numkeys = Integer.parseInt(toStr(argv[2]));
        } catch (NumberFormatException e) {
            return RespEncoder.encodeError("ERR value is not an integer or out of range");
        }
        if (numkeys < 0 || numkeys > argv.length - 3) {
            return RespEncoder.encodeError("ERR Number of keys can't be greater than number of args");
        }

        String[] keys = new String[numkeys];
        for (int i = 0; i < numkeys; i++) keys[i] = toStr(argv[3 + i]);
        String[] args = new String[argv.length - 3 - numkeys];
        for (int i = 0; i < args.length; i++) args[i] = toStr(argv[3 + numkeys + i]);

        return executeLua(client, script, keys, args);
    }

    // ---- EVALSHA ----

    @RedisCommand(name = "evalsha", arity = -3, flags = "noscript", firstKey = 0, lastKey = 0, step = 0)
    public byte[] evalsha(RedisClient client, byte[][] argv) {
        String sha1 = toStr(argv[1]).toLowerCase();
        String script = scriptCache.get(sha1);
        if (script == null) {
            return RespEncoder.encodeError("NOSCRIPT No matching script. Please use EVAL.");
        }
        // Reuse EVAL logic
        byte[][] newArgv = new byte[argv.length][];
        newArgv[0] = argv[0];
        newArgv[1] = script.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(argv, 2, newArgv, 2, argv.length - 2);
        return eval(client, newArgv);
    }

    // ---- SCRIPT ----

    @RedisCommand(name = "script", arity = -2, flags = "admin", firstKey = 0, lastKey = 0, step = 0)
    public byte[] script(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "LOAD":
                if (argv.length < 3) return RespEncoder.encodeError("ERR wrong number of arguments");
                String src = toStr(argv[2]);
                String sha = sha1hex(src);
                scriptCache.put(sha, src);
                return RespEncoder.encodeBulkString(sha.getBytes(StandardCharsets.UTF_8));
            case "EXISTS":
                List<Object> exists = new ArrayList<>();
                for (int i = 2; i < argv.length; i++) {
                    String s = toStr(argv[i]).toLowerCase();
                    exists.add(scriptCache.containsKey(s) ? 1L : 0L);
                }
                return RespEncoder.encodeArray(exists);
            case "FLUSH":
                scriptCache.clear();
                return RespEncoder.OK;
            default:
                return RespEncoder.encodeError("ERR unknown subcommand '" + subCmd + "' for 'script'");
        }
    }

    // ---- Lua execution engine ----

    private byte[] executeLua(RedisClient client, String script, String[] keys, String[] args) {
        try {
            Globals globals = JsePlatform.standardGlobals();

            // Build KEYS and ARGV tables
            LuaTable keysTable = new LuaTable();
            for (int i = 0; i < keys.length; i++) keysTable.set(i + 1, LuaValue.valueOf(keys[i]));
            LuaTable argsTable = new LuaTable();
            for (int i = 0; i < args.length; i++) argsTable.set(i + 1, LuaValue.valueOf(args[i]));

            globals.set("KEYS", keysTable);
            globals.set("ARGV", argsTable);

            // Build redis table with call() and pcall()
            LuaTable redisTable = new LuaTable();
            redisTable.set("call", new org.luaj.vm2.lib.VarArgFunction() {
                @Override
                public Varargs invoke(Varargs varargs) {
                    return redisCall(client, varargs, false);
                }
            });
            redisTable.set("pcall", new org.luaj.vm2.lib.VarArgFunction() {
                @Override
                public Varargs invoke(Varargs varargs) {
                    return redisCall(client, varargs, true);
                }
            });
            redisTable.set("error_reply", new org.luaj.vm2.lib.VarArgFunction() {
                @Override
                public Varargs invoke(Varargs varargs) {
                    LuaTable t = new LuaTable();
                    t.set("err", varargs.arg(1));
                    return t;
                }
            });
            redisTable.set("status_reply", new org.luaj.vm2.lib.VarArgFunction() {
                @Override
                public Varargs invoke(Varargs varargs) {
                    LuaTable t = new LuaTable();
                    t.set("ok", varargs.arg(1));
                    return t;
                }
            });
            globals.set("redis", redisTable);

            LuaValue chunk = globals.load(script);
            LuaValue result = chunk.call();
            return luaToResp(result);
        } catch (LuaError e) {
            return RespEncoder.encodeError("ERR Error running script: " + e.getMessage());
        } catch (Exception e) {
            return RespEncoder.encodeError("ERR Error running script: " + e.getMessage());
        }
    }

    private Varargs redisCall(RedisClient client, Varargs varargs, boolean pcall) {
        try {
            int n = varargs.narg();
            if (n == 0) return LuaValue.NIL;
            byte[][] cmdArgv = new byte[n][];
            for (int i = 1; i <= n; i++) {
                cmdArgv[i - 1] = varargs.arg(i).tojstring().getBytes(StandardCharsets.UTF_8);
            }
            byte[] result = server.executeCommand(client, cmdArgv);
            return respToLua(result);
        } catch (Exception e) {
            if (pcall) {
                LuaTable err = new LuaTable();
                err.set("err", LuaValue.valueOf(e.getMessage()));
                return err;
            }
            throw new LuaError(e.getMessage());
        }
    }

    /** Convert a Lua value to a RESP byte[]. */
    private byte[] luaToResp(LuaValue v) {
        if (v == null || v.isnil()) return RespEncoder.NULL_BULK;
        if (v.isboolean()) {
            return v.toboolean() ? RespEncoder.ONE : RespEncoder.NULL_BULK;
        }
        if (v.isint() || v.islong()) return RespEncoder.encodeInteger(v.tolong());
        if (v.isstring()) return RespEncoder.encodeBulkString(v.tojstring().getBytes(StandardCharsets.UTF_8));
        if (v.istable()) {
            LuaTable t = v.checktable();
            // Check for error reply
            LuaValue err = t.get("err");
            if (!err.isnil()) return RespEncoder.encodeError(err.tojstring());
            // Check for status reply
            LuaValue ok = t.get("ok");
            if (!ok.isnil()) return RespEncoder.encodeSimpleString(ok.tojstring());
            // Array
            int len = t.length();
            List<Object> items = new ArrayList<>();
            for (int i = 1; i <= len; i++) {
                LuaValue item = t.get(i);
                items.add(luaValueToObject(item));
            }
            return RespEncoder.encodeArray(items);
        }
        return RespEncoder.NULL_BULK;
    }

    private Object luaValueToObject(LuaValue v) {
        if (v == null || v.isnil()) return null;
        if (v.isint() || v.islong()) return v.tolong();
        if (v.isstring()) return v.tojstring().getBytes(StandardCharsets.UTF_8);
        return null;
    }

    /** Convert a RESP byte[] reply to a Lua value (for redis.call return). */
    private LuaValue respToLua(byte[] resp) {
        if (resp == null || resp.length == 0) return LuaValue.NIL;
        char type = (char) resp[0];
        String body = new String(resp, 1, resp.length - 1, StandardCharsets.UTF_8).trim();
        switch (type) {
            case '+': return LuaValue.valueOf(body.replace("\r\n", ""));
            case '-': {
                LuaTable err = new LuaTable();
                err.set("err", LuaValue.valueOf(body.replace("\r\n", "")));
                return err;
            }
            case ':': return LuaValue.valueOf(Long.parseLong(body.replace("\r\n", "")));
            case '$': {
                // Bulk string — extract value
                int crlf = body.indexOf("\r\n");
                if (crlf < 0) return LuaValue.NIL;
                int len = Integer.parseInt(body.substring(0, crlf));
                if (len < 0) return LuaValue.NIL;
                String value = body.substring(crlf + 2, crlf + 2 + len);
                return LuaValue.valueOf(value);
            }
            case '*': {
                // Array — simplified: return as table
                return LuaValue.NIL; // complex parsing omitted for brevity
            }
            default: return LuaValue.NIL;
        }
    }

    // ---- SHA1 utility ----

    public static String sha1hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(40);
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
