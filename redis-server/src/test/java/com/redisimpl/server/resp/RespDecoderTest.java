package com.redisimpl.server.resp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespDecoderTest {

    private RespDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new RespDecoder();
    }

    private ByteBuffer buf(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    @Test
    void decode_simpleArray() {
        // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        String cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        List<byte[][]> commands = decoder.decode(buf(cmd));
        assertEquals(1, commands.size());
        byte[][] args = commands.get(0);
        assertEquals(3, args.length);
        assertEquals("SET", new String(args[0], StandardCharsets.UTF_8));
        assertEquals("foo", new String(args[1], StandardCharsets.UTF_8));
        assertEquals("bar", new String(args[2], StandardCharsets.UTF_8));
    }

    @Test
    void decode_ping() {
        String cmd = "*1\r\n$4\r\nPING\r\n";
        List<byte[][]> commands = decoder.decode(buf(cmd));
        assertEquals(1, commands.size());
        assertEquals("PING", new String(commands.get(0)[0], StandardCharsets.UTF_8));
    }

    @Test
    void decode_multipleCommands() {
        String data = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        List<byte[][]> commands = decoder.decode(buf(data));
        assertEquals(2, commands.size());
    }

    @Test
    void decode_halfPacket_returnEmpty() {
        // Only part of a command
        String partial = "*3\r\n$3\r\nSET\r\n";
        List<byte[][]> commands = decoder.decode(buf(partial));
        assertEquals(0, commands.size());
    }

    @Test
    void decode_halfPacket_thenRemainder() {
        // First half
        String part1 = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n";
        List<byte[][]> commands1 = decoder.decode(buf(part1));
        assertEquals(0, commands1.size());

        // Second half
        String part2 = "$3\r\nbar\r\n";
        List<byte[][]> commands2 = decoder.decode(buf(part2));
        assertEquals(1, commands2.size());
        assertEquals("bar", new String(commands2.get(0)[2], StandardCharsets.UTF_8));
    }

    @Test
    void decode_inlineCommand_ping() {
        String inline = "PING\r\n";
        List<byte[][]> commands = decoder.decode(buf(inline));
        assertEquals(1, commands.size());
        assertEquals("PING", new String(commands.get(0)[0], StandardCharsets.UTF_8));
    }

    @Test
    void decode_inlineCommand_withArgs() {
        String inline = "SET foo bar\r\n";
        List<byte[][]> commands = decoder.decode(buf(inline));
        assertEquals(1, commands.size());
        byte[][] args = commands.get(0);
        assertEquals(3, args.length);
        assertEquals("SET", new String(args[0], StandardCharsets.UTF_8));
        assertEquals("foo", new String(args[1], StandardCharsets.UTF_8));
        assertEquals("bar", new String(args[2], StandardCharsets.UTF_8));
    }

    @Test
    void decode_binaryBulkString() {
        // Binary data with null bytes: *2\r\n$3\r\nKEY\r\n$4\r\n<binary4bytes>\r\n
        byte[] data = new byte[]{0, 1, 2, 3};
        String header = "*2\r\n$3\r\nKEY\r\n$4\r\n";
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] footer = "\r\n".getBytes(StandardCharsets.UTF_8);
        byte[] full = new byte[headerBytes.length + data.length + footer.length];
        System.arraycopy(headerBytes, 0, full, 0, headerBytes.length);
        System.arraycopy(data, 0, full, headerBytes.length, data.length);
        System.arraycopy(footer, 0, full, headerBytes.length + data.length, footer.length);
        List<byte[][]> commands = decoder.decode(ByteBuffer.wrap(full));
        assertEquals(1, commands.size());
        assertArrayEquals(data, commands.get(0)[1]);
    }

    @Test
    void decode_largeArray() {
        StringBuilder sb = new StringBuilder();
        sb.append("*5\r\n");
        for (int i = 1; i <= 5; i++) {
            String s = "item" + i;
            sb.append("$").append(s.length()).append("\r\n").append(s).append("\r\n");
        }
        List<byte[][]> commands = decoder.decode(buf(sb.toString()));
        assertEquals(1, commands.size());
        assertEquals(5, commands.get(0).length);
    }
}
