package com.redisimpl.server.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP2/3 streaming decoder.
 *
 * <p>Handles half-packets (incomplete data) and sticky packets (multiple commands in one buffer).
 * Supports:
 * <ul>
 *   <li>RESP2: *, $, +, -, :</li>
 *   <li>Inline commands (not starting with *)</li>
 * </ul>
 *
 * <p>State is preserved across calls to handle partial reads.
 */
public final class RespDecoder {

    /** Internal read buffer — accumulates data across calls */
    private byte[] readBuf = new byte[0];
    private int readPos = 0;

    /**
     * Decode one or more commands from the given buffer.
     * Returns a list of parsed commands (each is byte[][] of arguments).
     * Partial data is retained for the next call.
     */
    public List<byte[][]> decode(ByteBuffer data) {
        // Append new data to readBuf
        byte[] newData = new byte[data.remaining()];
        data.get(newData);
        appendToBuffer(newData);

        List<byte[][]> commands = new ArrayList<>();
        while (true) {
            int savedPos = readPos;
            byte[][] cmd = parseCommand();
            if (cmd == null) {
                // Incomplete — restore position
                readPos = savedPos;
                break;
            }
            commands.add(cmd);
        }

        // Compact buffer: discard consumed bytes
        if (readPos > 0) {
            int remaining = readBuf.length - readPos;
            byte[] newBuf = new byte[remaining];
            System.arraycopy(readBuf, readPos, newBuf, 0, remaining);
            readBuf = newBuf;
            readPos = 0;
        }

        return commands;
    }

    private void appendToBuffer(byte[] data) {
        int oldLen = readBuf.length - readPos;
        byte[] newBuf = new byte[oldLen + data.length];
        System.arraycopy(readBuf, readPos, newBuf, 0, oldLen);
        System.arraycopy(data, 0, newBuf, oldLen, data.length);
        readBuf = newBuf;
        readPos = 0;
    }

    /**
     * Try to parse one complete command from readBuf starting at readPos.
     * Returns null if insufficient data.
     * Advances readPos on success.
     */
    private byte[][] parseCommand() {
        if (readPos >= readBuf.length) return null;

        byte first = readBuf[readPos];
        if (first == '*') {
            return parseMultiBulk();
        } else {
            return parseInline();
        }
    }

    /**
     * Parse a RESP multi-bulk command: *N\r\n$len\r\ndata\r\n...
     */
    private byte[][] parseMultiBulk() {
        int start = readPos;
        // Read *N\r\n
        int lineEnd = findCRLF(readPos);
        if (lineEnd < 0) return null;

        String countLine = new String(readBuf, readPos + 1, lineEnd - readPos - 1, StandardCharsets.UTF_8);
        int argc;
        try {
            argc = Integer.parseInt(countLine.trim());
        } catch (NumberFormatException e) {
            return null;
        }
        readPos = lineEnd + 2; // skip \r\n

        if (argc <= 0) {
            return new byte[0][];
        }

        byte[][] argv = new byte[argc][];
        for (int i = 0; i < argc; i++) {
            if (readPos >= readBuf.length) {
                readPos = start;
                return null;
            }
            if (readBuf[readPos] != '$') {
                readPos = start;
                return null;
            }
            lineEnd = findCRLF(readPos);
            if (lineEnd < 0) {
                readPos = start;
                return null;
            }
            String lenStr = new String(readBuf, readPos + 1, lineEnd - readPos - 1, StandardCharsets.UTF_8);
            int len;
            try {
                len = Integer.parseInt(lenStr.trim());
            } catch (NumberFormatException e) {
                readPos = start;
                return null;
            }
            readPos = lineEnd + 2; // skip \r\n

            if (len < 0) {
                argv[i] = null;
                continue;
            }
            if (readPos + len + 2 > readBuf.length) {
                readPos = start;
                return null;
            }
            argv[i] = new byte[len];
            System.arraycopy(readBuf, readPos, argv[i], 0, len);
            readPos += len + 2; // skip data + \r\n
        }
        return argv;
    }

    /**
     * Parse an inline command: tokens separated by spaces, terminated by \r\n.
     */
    private byte[][] parseInline() {
        int lineEnd = findCRLF(readPos);
        if (lineEnd < 0) return null;

        String line = new String(readBuf, readPos, lineEnd - readPos, StandardCharsets.UTF_8);
        readPos = lineEnd + 2;

        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0 || (parts.length == 1 && parts[0].isEmpty())) {
            return new byte[0][];
        }
        byte[][] argv = new byte[parts.length][];
        for (int i = 0; i < parts.length; i++) {
            argv[i] = parts[i].getBytes(StandardCharsets.UTF_8);
        }
        return argv;
    }

    /**
     * Find the position of \r\n starting from {@code from}.
     * Returns the index of \r, or -1 if not found.
     */
    private int findCRLF(int from) {
        for (int i = from; i < readBuf.length - 1; i++) {
            if (readBuf[i] == '\r' && readBuf[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }

    /** Reset decoder state */
    public void reset() {
        readBuf = new byte[0];
        readPos = 0;
    }
}
