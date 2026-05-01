package com.redisimpl.server.commands.geo;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.zskiplist.ZSkipList;
import com.redisimpl.core.zskiplist.ZSkipListNode;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Geo command implementations: GEOADD, GEODIST, GEOPOS, GEOHASH, GEOSEARCH, GEOSEARCHSTORE.
 * Uses ZSet internally (geohash score).
 */
public final class GeoCommands {

    private final RedisServer server;

    public GeoCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) {
        return server.getDb(client.getDb());
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- GEOADD ----

    @RedisCommand(name = "geoadd", arity = -5, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] geoadd(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        int argIdx = 2;

        // Optional NX/XX/GT/LT/CH flags (skip for now)
        while (argIdx < argv.length) {
            String opt = toStr(argv[argIdx]).toUpperCase();
            if (opt.equals("NX") || opt.equals("XX") || opt.equals("GT") || opt.equals("LT") || opt.equals("CH")) {
                argIdx++;
            } else {
                break;
            }
        }

        if ((argv.length - argIdx) % 3 != 0) {
            return RespEncoder.encodeError("ERR syntax error");
        }

        // Build ZADD argv: ZADD key score member [score member ...]
        int numMembers = (argv.length - argIdx) / 3;
        byte[][] zaddArgv = new byte[3 + numMembers * 2][];
        zaddArgv[0] = "zadd".getBytes(StandardCharsets.UTF_8);
        zaddArgv[1] = key;
        zaddArgv[2] = "XX".getBytes(StandardCharsets.UTF_8); // placeholder, will be replaced

        // Actually build proper zadd argv
        byte[][] realZaddArgv = new byte[2 + numMembers * 2][];
        realZaddArgv[0] = "zadd".getBytes(StandardCharsets.UTF_8);
        realZaddArgv[1] = key;

        for (int i = 0; i < numMembers; i++) {
            double lon = Double.parseDouble(toStr(argv[argIdx + i * 3]));
            double lat = Double.parseDouble(toStr(argv[argIdx + i * 3 + 1]));
            byte[] member = argv[argIdx + i * 3 + 2];

            if (lon < -180 || lon > 180 || lat < -85.05112878 || lat > 85.05112878) {
                return RespEncoder.encodeError("ERR invalid longitude,latitude pair " + lon + "," + lat);
            }

            double score = GeoHash.encode(lon, lat);
            realZaddArgv[2 + i * 2] = String.valueOf(score).getBytes(StandardCharsets.UTF_8);
            realZaddArgv[2 + i * 2 + 1] = member;
        }

        return server.executeCommand(client, realZaddArgv);
    }

    // ---- GEODIST ----

    @RedisCommand(name = "geodist", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] geodist(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        byte[] member1 = argv[2];
        byte[] member2 = argv[3];
        String unit = argv.length >= 5 ? toStr(argv[4]) : "m";

        double[] pos1 = getPos(db(client), key, member1);
        double[] pos2 = getPos(db(client), key, member2);

        if (pos1 == null || pos2 == null) return RespEncoder.NULL_BULK;

        double dist = GeoHash.distance(pos1[0], pos1[1], pos2[0], pos2[1]);
        double converted = GeoHash.toUnit(dist, unit);
        return RespEncoder.encodeBulkString(String.format("%.4f", converted).getBytes(StandardCharsets.UTF_8));
    }

    // ---- GEOPOS ----

    @RedisCommand(name = "geopos", arity = -2, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] geopos(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + (argv.length - 2) + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (int i = 2; i < argv.length; i++) {
                double[] pos = getPos(db(client), key, argv[i]);
                if (pos == null) {
                    out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
                } else {
                    out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
                    out.write(RespEncoder.encodeBulkString(String.format("%.17g", pos[0]).getBytes(StandardCharsets.UTF_8)));
                    out.write(RespEncoder.encodeBulkString(String.format("%.17g", pos[1]).getBytes(StandardCharsets.UTF_8)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- GEOHASH ----

    @RedisCommand(name = "geohash", arity = -2, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] geohash(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + (argv.length - 2) + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (int i = 2; i < argv.length; i++) {
                double[] pos = getPos(db(client), key, argv[i]);
                if (pos == null) {
                    out.write(RespEncoder.NULL_BULK);
                } else {
                    String hash = GeoHash.encodeString(pos[0], pos[1]);
                    out.write(RespEncoder.encodeBulkString(hash.getBytes(StandardCharsets.UTF_8)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- GEOSEARCH ----

    @RedisCommand(name = "geosearch", arity = -7, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] geosearch(RedisClient client, byte[][] argv) {
        return geosearchImpl(client, argv, null);
    }

    // ---- GEOSEARCHSTORE ----

    @RedisCommand(name = "geosearchstore", arity = -8, flags = "write denyoom", firstKey = 1, lastKey = 2, step = 1)
    public byte[] geosearchstore(RedisClient client, byte[][] argv) {
        byte[] destKey = argv[1];
        // Shift argv: GEOSEARCHSTORE dest source FROMMEMBER/FROMLONLAT ...
        byte[][] searchArgv = new byte[argv.length - 1][];
        searchArgv[0] = "geosearch".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(argv, 2, searchArgv, 1, argv.length - 2);
        return geosearchImpl(client, searchArgv, destKey);
    }

    private byte[] geosearchImpl(RedisClient client, byte[][] argv, byte[] destKey) {
        byte[] key = argv[1];
        int argIdx = 2;

        // Parse FROMMEMBER or FROMLONLAT
        double centerLon, centerLat;
        String fromOpt = toStr(argv[argIdx]).toUpperCase();
        if ("FROMMEMBER".equals(fromOpt)) {
            argIdx++;
            double[] pos = getPos(db(client), key, argv[argIdx++]);
            if (pos == null) return RespEncoder.encodeError("ERR could not hget the element");
            centerLon = pos[0];
            centerLat = pos[1];
        } else if ("FROMLONLAT".equals(fromOpt)) {
            argIdx++;
            centerLon = Double.parseDouble(toStr(argv[argIdx++]));
            centerLat = Double.parseDouble(toStr(argv[argIdx++]));
        } else {
            return RespEncoder.encodeError("ERR syntax error");
        }

        // Parse BYRADIUS or BYBOX
        String byOpt = toStr(argv[argIdx]).toUpperCase();
        double radiusMeters;
        if ("BYRADIUS".equals(byOpt)) {
            argIdx++;
            double radius = Double.parseDouble(toStr(argv[argIdx++]));
            String unit = toStr(argv[argIdx++]);
            radiusMeters = GeoHash.toMeters(radius, unit);
        } else if ("BYBOX".equals(byOpt)) {
            argIdx++;
            double width  = Double.parseDouble(toStr(argv[argIdx++]));
            double height = Double.parseDouble(toStr(argv[argIdx++]));
            String unit   = toStr(argv[argIdx++]);
            // Use max dimension as radius approximation
            radiusMeters = GeoHash.toMeters(Math.max(width, height) / 2.0, unit);
        } else {
            return RespEncoder.encodeError("ERR syntax error");
        }

        // Parse ASC/DESC, COUNT
        boolean asc = true;
        int count = Integer.MAX_VALUE;
        boolean withCoord = false, withDist = false, withHash = false;
        while (argIdx < argv.length) {
            String opt = toStr(argv[argIdx]).toUpperCase();
            switch (opt) {
                case "ASC":   asc = true;  argIdx++; break;
                case "DESC":  asc = false; argIdx++; break;
                case "COUNT": argIdx++; count = Integer.parseInt(toStr(argv[argIdx++])); break;
                case "WITHCOORD": withCoord = true; argIdx++; break;
                case "WITHDIST":  withDist  = true; argIdx++; break;
                case "WITHHASH":  withHash  = true; argIdx++; break;
                case "STOREDIST": argIdx++; break; // for GEOSEARCHSTORE
                default: argIdx++;
            }
        }

        // Get all members from the ZSet and filter by distance
        RedisObject obj = db(client).lookupKey(key);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        if (obj.getType() != RedisObjectConstants.OBJ_TYPE_ZSET) {
            throw new RedisException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<GeoResult> results = new ArrayList<>();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i + 1 < lp.size(); i += 2) {
                byte[] member = lp.get(i);
                double score = Double.parseDouble(new String(lp.get(i + 1), StandardCharsets.UTF_8));
                double[] pos = GeoHash.decode(score);
                double dist = GeoHash.distance(centerLon, centerLat, pos[0], pos[1]);
                if (dist <= radiusMeters) {
                    results.add(new GeoResult(member, pos[0], pos[1], dist, score));
                }
            }
        } else {
            ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) obj.getPtr();
            ZSkipListNode node = data.zsl.getHeader().getLevels()[0].forward;
            while (node != null) {
                double[] pos = GeoHash.decode(node.getScore());
                double dist = GeoHash.distance(centerLon, centerLat, pos[0], pos[1]);
                if (dist <= radiusMeters) {
                    results.add(new GeoResult(node.getEle(), pos[0], pos[1], dist, node.getScore()));
                }
                node = node.getLevels()[0].forward;
            }
        }

        // Sort
        final boolean ascFinal = asc;
        results.sort((a, b) -> ascFinal ? Double.compare(a.dist, b.dist) : Double.compare(b.dist, a.dist));
        if (results.size() > count) results = results.subList(0, count);

        // If GEOSEARCHSTORE, store results
        if (destKey != null) {
            byte[][] zaddArgv = new byte[2 + results.size() * 2][];
            zaddArgv[0] = "zadd".getBytes(StandardCharsets.UTF_8);
            zaddArgv[1] = destKey;
            for (int i = 0; i < results.size(); i++) {
                zaddArgv[2 + i * 2] = String.valueOf(results.get(i).score).getBytes(StandardCharsets.UTF_8);
                zaddArgv[2 + i * 2 + 1] = results.get(i).member;
            }
            server.executeCommand(client, zaddArgv);
            return RespEncoder.encodeInteger(results.size());
        }

        // Encode response
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + results.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (GeoResult r : results) {
                boolean needArray = withCoord || withDist || withHash;
                if (!needArray) {
                    out.write(RespEncoder.encodeBulkString(r.member));
                } else {
                    int fields = 1 + (withDist ? 1 : 0) + (withHash ? 1 : 0) + (withCoord ? 1 : 0);
                    out.write(("*" + fields + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(RespEncoder.encodeBulkString(r.member));
                    if (withDist) {
                        String unit = "m"; // default
                        out.write(RespEncoder.encodeBulkString(
                            String.format("%.4f", r.dist).getBytes(StandardCharsets.UTF_8)));
                    }
                    if (withHash) {
                        out.write(RespEncoder.encodeInteger((long) r.score));
                    }
                    if (withCoord) {
                        out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
                        out.write(RespEncoder.encodeBulkString(
                            String.format("%.17g", r.lon).getBytes(StandardCharsets.UTF_8)));
                        out.write(RespEncoder.encodeBulkString(
                            String.format("%.17g", r.lat).getBytes(StandardCharsets.UTF_8)));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- Helper: get position of a member ----

    private double[] getPos(RedisDb db, byte[] key, byte[] member) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_ZSET) return null;

        Double score = zsetScoreDirect(obj, member);
        if (score == null) return null;
        return GeoHash.decode(score);
    }

    /** Get the score of a member directly from a ZSet object. */
    private static Double zsetScoreDirect(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i + 1 < lp.size(); i += 2) {
                if (java.util.Arrays.equals(lp.get(i), member)) {
                    return Double.parseDouble(new String(lp.get(i + 1), StandardCharsets.UTF_8));
                }
            }
            return null;
        }
        ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) obj.getPtr();
        Object v = data.dict.get(member);
        return v != null ? (Double) v : null;
    }

    // ---- Inner class for search results ----

    private static final class GeoResult {
        final byte[] member;
        final double lon, lat, dist, score;

        GeoResult(byte[] member, double lon, double lat, double dist, double score) {
            this.member = member;
            this.lon = lon;
            this.lat = lat;
            this.dist = dist;
            this.score = score;
        }
    }
}
