package com.redisimpl.server.commands.hyperloglog;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.sds.Sds;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.commands.string.StringCommands;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * HyperLogLog command implementations: PFADD, PFCOUNT, PFMERGE.
 */
public final class HyperLogLogCommands {

    private final RedisServer server;

    public HyperLogLogCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) {
        return server.getDb(client.getDb());
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- Get or create HLL blob for a key ----

    private byte[] getHllRegs(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null) return null;
        if (obj.getType() != RedisObjectConstants.OBJ_TYPE_STRING) {
            throw new RedisException("WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        byte[] raw = StringCommands.getStringValue(obj);
        if (!HyperLogLog.isHll(raw)) {
            throw new RedisException("WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        return raw;
    }

    private byte[] getOrCreateHllRegs(RedisDb db, byte[] key) {
        byte[] regs = getHllRegs(db, key);
        if (regs == null) {
            regs = HyperLogLog.create();
            db.setKey(key, RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                Sds.fromBytes(regs)));
        }
        return regs;
    }

    private void saveHllRegs(RedisDb db, byte[] key, byte[] regs) {
        db.setKey(key, RedisObject.createObject(
            RedisObjectConstants.OBJ_TYPE_STRING,
            RedisObjectConstants.OBJ_ENCODING_RAW,
            Sds.fromBytes(regs)));
    }

    // ---- PFADD ----

    @RedisCommand(name = "pfadd", arity = -2, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] pfadd(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        RedisDb db = db(client);
        byte[] regs = getOrCreateHllRegs(db, key);

        boolean changed = false;
        for (int i = 2; i < argv.length; i++) {
            if (HyperLogLog.add(regs, argv[i])) changed = true;
        }

        if (changed) {
            saveHllRegs(db, key, regs);
            return RespEncoder.ONE;
        }
        return RespEncoder.ZERO;
    }

    // ---- PFCOUNT ----

    @RedisCommand(name = "pfcount", arity = -2, flags = "read-only", firstKey = 1, lastKey = -1, step = 1)
    public byte[] pfcount(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);

        if (argv.length == 2) {
            // Single key
            byte[] regs = getHllRegs(db, argv[1]);
            if (regs == null) return RespEncoder.ZERO;
            return RespEncoder.encodeInteger(HyperLogLog.count(regs));
        }

        // Multiple keys: merge into a temp HLL
        byte[] merged = HyperLogLog.create();
        for (int i = 1; i < argv.length; i++) {
            byte[] regs = getHllRegs(db, argv[i]);
            if (regs != null) HyperLogLog.merge(merged, regs);
        }
        return RespEncoder.encodeInteger(HyperLogLog.count(merged));
    }

    // ---- PFMERGE ----

    @RedisCommand(name = "pfmerge", arity = -2, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 1)
    public byte[] pfmerge(RedisClient client, byte[][] argv) {
        byte[] destKey = argv[1];
        RedisDb db = db(client);

        byte[] dest = getOrCreateHllRegs(db, destKey);
        for (int i = 2; i < argv.length; i++) {
            byte[] src = getHllRegs(db, argv[i]);
            if (src != null) HyperLogLog.merge(dest, src);
        }
        saveHllRegs(db, destKey, dest);
        return RespEncoder.OK;
    }
}
