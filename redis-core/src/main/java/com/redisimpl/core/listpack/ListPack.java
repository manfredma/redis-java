package com.redisimpl.core.listpack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * ListPack — Java port of Redis's listpack.c with full binary encoding.
 *
 * <p>Binary format (identical to Redis listpack):
 * <pre>
 * Header (6 bytes):
 *   [0-3] total-bytes  (uint32_t, little-endian)
 *   [4-5] num-elements (uint16_t, little-endian, 0xFFFF = unknown)
 * Entries:
 *   each entry = encoding_bytes + data_bytes + backlen_bytes
 * EOF:
 *   0xFF
 * </pre>
 *
 * <p>Encoding types:
 * <pre>
 *   7BIT_UINT:   1 byte  — 0xxxxxxx (0-127)
 *   6BIT_STR:    1+len   — 10xxxxxx (len < 64)
 *   13BIT_INT:   2 bytes — 110xxxxx xxxxxxxx (-4096..4095)
 *   12BIT_STR:   2+len   — 1110xxxx xxxxxxxx (len < 4096)
 *   16BIT_INT:   3 bytes — 0xF1 + 2 bytes LE
 *   24BIT_INT:   4 bytes — 0xF2 + 3 bytes LE
 *   32BIT_INT:   5 bytes — 0xF3 + 4 bytes LE
 *   64BIT_INT:   9 bytes — 0xF4 + 8 bytes LE
 *   32BIT_STR:   5+len   — 0xF0 + 4 bytes LE len
 * </pre>
 *
 * <p>Immutable semantics: all mutating operations return a new ListPack.
 */
public final class ListPack {

    // ---- Constants ----

    /** Maximum entries before conversion to QuickList */
    public static final int LIST_MAX_LISTPACK_SIZE = 128;
    /** Maximum element size before conversion to QuickList */
    public static final int LIST_MAX_LISTPACK_VALUE = 64;

    private static final int LP_HDR_SIZE = 6;
    private static final int LP_HDR_NUMELE_UNKNOWN = 0xFFFF;
    private static final int LP_EOF = 0xFF;

    // Encoding type constants
    private static final int LP_ENCODING_7BIT_UINT      = 0x00;
    private static final int LP_ENCODING_7BIT_UINT_MASK = 0x80;
    private static final int LP_ENCODING_6BIT_STR       = 0x80;
    private static final int LP_ENCODING_6BIT_STR_MASK  = 0xC0;
    private static final int LP_ENCODING_13BIT_INT      = 0xC0;
    private static final int LP_ENCODING_13BIT_INT_MASK = 0xE0;
    private static final int LP_ENCODING_12BIT_STR      = 0xE0;
    private static final int LP_ENCODING_12BIT_STR_MASK = 0xF0;
    private static final int LP_ENCODING_16BIT_INT      = 0xF1;
    private static final int LP_ENCODING_24BIT_INT      = 0xF2;
    private static final int LP_ENCODING_32BIT_INT      = 0xF3;
    private static final int LP_ENCODING_64BIT_INT      = 0xF4;
    private static final int LP_ENCODING_32BIT_STR      = 0xF0;

    // LONG_STR_SIZE from Redis util.h: 21 chars is max for int64 decimal string
    private static final int LONG_STR_SIZE = 21;

    // ---- Internal storage ----

    /** Raw binary data of the listpack, including header and EOF. */
    private final byte[] data;

    // ---- Constructors ----

    private ListPack(byte[] data) {
        this.data = data;
    }

    // ---- Factory ----

    /**
     * Create a new empty listpack (lpNew equivalent).
     * Binary: [7,0,0,0, 0,0, 0xFF] = 7 bytes
     */
    public static ListPack create() {
        byte[] d = new byte[LP_HDR_SIZE + 1];
        setTotalBytes(d, LP_HDR_SIZE + 1);
        setNumElements(d, 0);
        d[LP_HDR_SIZE] = (byte) LP_EOF;
        return new ListPack(d);
    }

    /**
     * Load a listpack from raw binary data (fromBytes equivalent).
     */
    public static ListPack fromBytes(byte[] raw) {
        return new ListPack(raw.clone());
    }

    // ---- Core operations ----

    /**
     * Append a string element to the end. Returns a new ListPack.
     */
    public ListPack append(byte[] element) {
        return insertString(element, eofOffset(), LP_BEFORE);
    }

    /**
     * Append an integer element to the end. Returns a new ListPack.
     */
    public ListPack appendInteger(long v) {
        return insertInteger(v, eofOffset(), LP_BEFORE);
    }

    /**
     * Prepend a string element to the beginning. Returns a new ListPack.
     */
    public ListPack prepend(byte[] element) {
        int firstPos = firstOffset();
        if (firstPos == -1) {
            return append(element);
        }
        return insertString(element, firstPos, LP_BEFORE);
    }

    /**
     * Prepend an integer element to the beginning. Returns a new ListPack.
     */
    public ListPack prependInteger(long v) {
        int firstPos = firstOffset();
        if (firstPos == -1) {
            return appendInteger(v);
        }
        return insertInteger(v, firstPos, LP_BEFORE);
    }

    /**
     * Insert a string element at the given index (LP_BEFORE semantics). Returns a new ListPack.
     */
    public ListPack insert(int index, byte[] element) {
        int pos = seekOffset(index);
        if (pos == -1) {
            // index == size(): append at end
            if (index == size()) {
                return append(element);
            }
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return insertString(element, pos, LP_BEFORE);
    }

    /**
     * Delete element at the given index. Returns a new ListPack.
     */
    public ListPack delete(int index) {
        int pos = seekOffset(index);
        if (pos == -1) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return deleteAt(pos);
    }

    /**
     * Replace element at the given index with a new string. Returns a new ListPack.
     */
    public ListPack replace(int index, byte[] element) {
        int pos = seekOffset(index);
        if (pos == -1) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return insertString(element, pos, LP_REPLACE);
    }

    /**
     * Replace element at the given index (alias for replace). Returns a new ListPack.
     */
    public ListPack set(int index, byte[] element) {
        return replace(index, element);
    }

    // ---- Accessors ----

    /**
     * Get element at index as byte[]. Throws IndexOutOfBoundsException if out of range.
     * Only non-negative indexes are accepted (unlike lpSeek which supports negative).
     * Integer-encoded entries are returned as their decimal string representation.
     */
    public byte[] get(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        int pos = seekOffset(index);
        if (pos == -1) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return getAt(pos);
    }

    /**
     * Get element at index as long. Throws NumberFormatException if not parseable as integer.
     */
    public long getLong(int index) {
        int pos = seekOffset(index);
        if (pos == -1) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return getLongAt(pos);
    }

    /**
     * Number of elements (lpLength equivalent).
     */
    public int size() {
        int n = getNumElements(data);
        if (n != LP_HDR_NUMELE_UNKNOWN) return n;
        // Full scan
        int count = 0;
        int pos = firstOffset();
        while (pos != -1) {
            count++;
            pos = nextOffset(pos);
        }
        return count;
    }

    /**
     * Return all elements as a new list (defensive copies, integers as decimal strings).
     */
    public List<byte[]> toList() {
        List<byte[]> result = new ArrayList<>();
        int pos = firstOffset();
        while (pos != -1) {
            result.add(getAt(pos));
            pos = nextOffset(pos);
        }
        return result;
    }

    /**
     * Find the first occurrence of element (byte-by-byte comparison).
     * Returns index or -1 if not found.
     */
    public int indexOf(byte[] element) {
        int idx = 0;
        int pos = firstOffset();
        while (pos != -1) {
            if (Arrays.equals(getAt(pos), element)) return idx;
            idx++;
            pos = nextOffset(pos);
        }
        return -1;
    }

    /**
     * Find the last occurrence of element.
     * Returns index or -1 if not found.
     */
    public int lastIndexOf(byte[] element) {
        int idx = 0;
        int found = -1;
        int pos = firstOffset();
        while (pos != -1) {
            if (Arrays.equals(getAt(pos), element)) found = idx;
            idx++;
            pos = nextOffset(pos);
        }
        return found;
    }

    /**
     * Return the raw binary data of this listpack (copy).
     */
    public byte[] getBytes() {
        return data.clone();
    }

    /**
     * Total bytes in the listpack (lpGetTotalBytes).
     */
    public int totalBytes() {
        return getTotalBytes(data);
    }

    /**
     * Number of elements stored in the header (lpGetNumElements).
     * May return LP_HDR_NUMELE_UNKNOWN (0xFFFF) if count overflowed.
     */
    public int numElements() {
        return getNumElements(data);
    }

    // ---- Position-based API (lpFirst / lpNext / lpPrev / lpSeek) ----

    /**
     * Returns the byte offset of the first element, or -1 if empty.
     */
    public int first() {
        return firstOffset();
    }

    /**
     * Returns the byte offset of the next element after pos, or -1 if at end.
     */
    public int next(int pos) {
        return nextOffset(pos);
    }

    /**
     * Returns the byte offset of the previous element before pos, or -1 if at start.
     */
    public int prev(int pos) {
        return prevOffset(pos);
    }

    /**
     * Read the entry at byte offset pos as byte[] (integers returned as decimal strings).
     */
    public byte[] getAt(int pos) {
        return readEntryAsBytes(pos);
    }

    /**
     * Read the entry at byte offset pos as long. Throws NumberFormatException if not integer.
     */
    public long getLongAt(int pos) {
        return readEntryAsLong(pos);
    }

    // ---- Iterator ----

    /**
     * Create a forward iterator over all elements.
     */
    public ListPackIterator iterator() {
        return new ListPackIterator(this, false);
    }

    /**
     * Create a reverse iterator over all elements.
     */
    public ListPackIterator reverseIterator() {
        return new ListPackIterator(this, true);
    }

    // =========================================================
    // Internal: position/offset helpers
    // =========================================================

    private int firstOffset() {
        int pos = LP_HDR_SIZE;
        if ((data[pos] & 0xFF) == LP_EOF) return -1;
        return pos;
    }

    private int nextOffset(int pos) {
        int entryLen = currentEncodedSizeUnsafe(pos);
        entryLen += encodeBacklenBytes(entryLen);
        int next = pos + entryLen;
        if ((data[next] & 0xFF) == LP_EOF) return -1;
        return next;
    }

    private int prevOffset(int pos) {
        if (pos == LP_HDR_SIZE) return -1;
        // Read backlen from the byte just before pos
        int backlenEnd = pos - 1;
        long prevlen = decodeBacklen(backlenEnd);
        long backlenSize = encodeBacklenBytes(prevlen);
        long entryStart = pos - prevlen - backlenSize;
        return (int) entryStart;
    }

    private int eofOffset() {
        return getTotalBytes(data) - 1;
    }

    private int seekOffset(int index) {
        int n = getNumElements(data);
        if (n != LP_HDR_NUMELE_UNKNOWN) {
            if (index < 0) index = n + index;
            if (index < 0 || index >= n) return -1;
            // Seek from whichever end is closer
            if (index <= n / 2) {
                int pos = firstOffset();
                for (int i = 0; i < index && pos != -1; i++) {
                    pos = nextOffset(pos);
                }
                return pos;
            } else {
                // seek from last
                int pos = lastOffset();
                for (int i = n - 1; i > index && pos != -1; i--) {
                    pos = prevOffset(pos);
                }
                return pos;
            }
        } else {
            // Unknown count: scan from front for positive, back for negative
            if (index >= 0) {
                int pos = firstOffset();
                for (int i = 0; i < index && pos != -1; i++) {
                    pos = nextOffset(pos);
                }
                return pos;
            } else {
                // negative: need full scan first
                int count = size();
                int adjusted = count + index;
                if (adjusted < 0) return -1;
                return seekOffset(adjusted);
            }
        }
    }

    private int lastOffset() {
        // Walk from EOF backwards
        int eofPos = eofOffset();
        return prevOffset(eofPos); // prevOffset works from the byte before the entry
    }

    // =========================================================
    // Internal: entry reading
    // =========================================================

    /**
     * Read entry at pos as byte[]. Integer entries are converted to decimal string bytes.
     */
    private byte[] readEntryAsBytes(int pos) {
        int enc = data[pos] & 0xFF;

        if (is7BitUint(enc)) {
            long val = enc & 0x7F;
            return Long.toString(val).getBytes();
        } else if (is6BitStr(enc)) {
            int len = enc & 0x3F;
            byte[] result = new byte[len];
            System.arraycopy(data, pos + 1, result, 0, len);
            return result;
        } else if (is13BitInt(enc)) {
            int uval = ((enc & 0x1F) << 8) | (data[pos + 1] & 0xFF);
            long val = toSigned13(uval);
            return Long.toString(val).getBytes();
        } else if (is16BitInt(enc)) {
            int uval = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8);
            long val = toSigned16(uval);
            return Long.toString(val).getBytes();
        } else if (is24BitInt(enc)) {
            int uval = (data[pos + 1] & 0xFF)
                    | ((data[pos + 2] & 0xFF) << 8)
                    | ((data[pos + 3] & 0xFF) << 16);
            long val = toSigned24(uval);
            return Long.toString(val).getBytes();
        } else if (is32BitInt(enc)) {
            long uval = (data[pos + 1] & 0xFFL)
                    | ((data[pos + 2] & 0xFFL) << 8)
                    | ((data[pos + 3] & 0xFFL) << 16)
                    | ((data[pos + 4] & 0xFFL) << 24);
            long val = toSigned32(uval);
            return Long.toString(val).getBytes();
        } else if (is64BitInt(enc)) {
            long uval = (data[pos + 1] & 0xFFL)
                    | ((data[pos + 2] & 0xFFL) << 8)
                    | ((data[pos + 3] & 0xFFL) << 16)
                    | ((data[pos + 4] & 0xFFL) << 24)
                    | ((data[pos + 5] & 0xFFL) << 32)
                    | ((data[pos + 6] & 0xFFL) << 40)
                    | ((data[pos + 7] & 0xFFL) << 48)
                    | ((data[pos + 8] & 0xFFL) << 56);
            return Long.toString(uval).getBytes();
        } else if (is12BitStr(enc)) {
            int len = ((enc & 0x0F) << 8) | (data[pos + 1] & 0xFF);
            byte[] result = new byte[len];
            System.arraycopy(data, pos + 2, result, 0, len);
            return result;
        } else if (is32BitStr(enc)) {
            int len = (data[pos + 1] & 0xFF)
                    | ((data[pos + 2] & 0xFF) << 8)
                    | ((data[pos + 3] & 0xFF) << 16)
                    | ((data[pos + 4] & 0xFF) << 24);
            byte[] result = new byte[len];
            System.arraycopy(data, pos + 5, result, 0, len);
            return result;
        }
        throw new IllegalStateException("Unknown encoding at pos " + pos + ": 0x" + Integer.toHexString(enc));
    }

    /**
     * Read entry at pos as long. Throws NumberFormatException if not integer-encoded
     * and not parseable as integer string.
     */
    private long readEntryAsLong(int pos) {
        int enc = data[pos] & 0xFF;

        if (is7BitUint(enc)) {
            return enc & 0x7FL;
        } else if (is13BitInt(enc)) {
            int uval = ((enc & 0x1F) << 8) | (data[pos + 1] & 0xFF);
            return toSigned13(uval);
        } else if (is16BitInt(enc)) {
            int uval = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8);
            return toSigned16(uval);
        } else if (is24BitInt(enc)) {
            int uval = (data[pos + 1] & 0xFF)
                    | ((data[pos + 2] & 0xFF) << 8)
                    | ((data[pos + 3] & 0xFF) << 16);
            return toSigned24(uval);
        } else if (is32BitInt(enc)) {
            long uval = (data[pos + 1] & 0xFFL)
                    | ((data[pos + 2] & 0xFFL) << 8)
                    | ((data[pos + 3] & 0xFFL) << 16)
                    | ((data[pos + 4] & 0xFFL) << 24);
            return toSigned32(uval);
        } else if (is64BitInt(enc)) {
            return (data[pos + 1] & 0xFFL)
                    | ((data[pos + 2] & 0xFFL) << 8)
                    | ((data[pos + 3] & 0xFFL) << 16)
                    | ((data[pos + 4] & 0xFFL) << 24)
                    | ((data[pos + 5] & 0xFFL) << 32)
                    | ((data[pos + 6] & 0xFFL) << 40)
                    | ((data[pos + 7] & 0xFFL) << 48)
                    | ((data[pos + 8] & 0xFFL) << 56);
        } else {
            // String encoding: try to parse as integer
            byte[] strBytes = readEntryAsBytes(pos);
            long[] result = new long[1];
            if (!lpStringToInt64(strBytes, strBytes.length, result)) {
                throw new NumberFormatException("Not an integer: " + new String(strBytes));
            }
            return result[0];
        }
    }

    // =========================================================
    // Internal: entry size calculation
    // =========================================================

    /** Returns the encoded size (encoding + data bytes, WITHOUT backlen). */
    private int currentEncodedSizeUnsafe(int pos) {
        int enc = data[pos] & 0xFF;
        if (is7BitUint(enc)) return 1;
        if (is6BitStr(enc)) return 1 + (enc & 0x3F);
        if (is13BitInt(enc)) return 2;
        if (is16BitInt(enc)) return 3;
        if (is24BitInt(enc)) return 4;
        if (is32BitInt(enc)) return 5;
        if (is64BitInt(enc)) return 9;
        if (is12BitStr(enc)) return 2 + (((enc & 0x0F) << 8) | (data[pos + 1] & 0xFF));
        if (is32BitStr(enc)) {
            int len = (data[pos + 1] & 0xFF)
                    | ((data[pos + 2] & 0xFF) << 8)
                    | ((data[pos + 3] & 0xFF) << 16)
                    | ((data[pos + 4] & 0xFF) << 24);
            return 5 + len;
        }
        if (enc == LP_EOF) return 1;
        return 0;
    }

    // =========================================================
    // Internal: insert/delete operations
    // =========================================================

    private static final int LP_BEFORE  = 0;
    private static final int LP_AFTER   = 1;
    private static final int LP_REPLACE = 2;

    /**
     * Insert a string element at position pos with the given where flag.
     * Returns a new ListPack.
     */
    private ListPack insertString(byte[] ele, int pos, int where) {
        // Determine encoding type and encoded length
        long[] enclen = new long[1];
        byte[] intenc = new byte[9];
        int enctype = encodeGetType(ele, ele.length, intenc, enclen);

        if (enctype == 0 /* LP_ENCODING_INT */) {
            return doInsert(intenc, (int) enclen[0], pos, where, false);
        } else {
            return doInsertString(ele, pos, where);
        }
    }

    /**
     * Insert an integer element at position pos with the given where flag.
     * Returns a new ListPack.
     */
    private ListPack insertInteger(long v, int pos, int where) {
        byte[] intenc = new byte[9];
        long[] enclen = new long[1];
        encodeIntegerGetType(v, intenc, enclen);
        return doInsert(intenc, (int) enclen[0], pos, where, false);
    }

    /**
     * Delete entry at position pos. Returns a new ListPack.
     */
    private ListPack deleteAt(int pos) {
        return doInsert(null, 0, pos, LP_REPLACE, true);
    }

    /**
     * Core insert/replace/delete operation on the raw byte array.
     *
     * @param encBytes  encoded bytes to insert (null for delete)
     * @param encLen    length of encBytes
     * @param pos       byte offset in data[] where operation takes place
     * @param where     LP_BEFORE, LP_AFTER, or LP_REPLACE
     * @param isDelete  true if this is a delete operation
     */
    private ListPack doInsert(byte[] encBytes, int encLen, int pos, int where, boolean isDelete) {
        if (where == LP_AFTER) {
            pos = pos + currentEncodedSizeUnsafe(pos) + (int) encodeBacklenBytes(currentEncodedSizeUnsafe(pos));
            where = LP_BEFORE;
        }

        // Compute backlen
        int backlenSize = isDelete ? 0 : (int) encodeBacklen(null, encLen);
        byte[] backlenBuf = new byte[5];
        if (!isDelete) {
            encodeBacklen(backlenBuf, encLen);
        }

        int oldTotal = getTotalBytes(data);
        int replacedLen = 0;
        if (where == LP_REPLACE) {
            replacedLen = currentEncodedSizeUnsafe(pos);
            replacedLen += (int) encodeBacklenBytes(replacedLen);
        }

        int newTotal = oldTotal + encLen + backlenSize - replacedLen;

        // Build new byte array
        byte[] newData = new byte[newTotal];

        if (where == LP_BEFORE) {
            // Copy everything before pos
            System.arraycopy(data, 0, newData, 0, pos);
            // Write new entry
            if (!isDelete) {
                System.arraycopy(encBytes, 0, newData, pos, encLen);
                System.arraycopy(backlenBuf, 0, newData, pos + encLen, backlenSize);
            }
            // Copy everything from pos to end
            System.arraycopy(data, pos, newData, pos + encLen + backlenSize, oldTotal - pos);
        } else { // LP_REPLACE
            // Copy everything before pos
            System.arraycopy(data, 0, newData, 0, pos);
            // Write new entry (nothing if delete)
            if (!isDelete) {
                System.arraycopy(encBytes, 0, newData, pos, encLen);
                System.arraycopy(backlenBuf, 0, newData, pos + encLen, backlenSize);
            }
            // Copy everything after the replaced entry
            int srcStart = pos + replacedLen;
            int dstStart = pos + encLen + backlenSize;
            System.arraycopy(data, srcStart, newData, dstStart, oldTotal - srcStart);
        }

        // Update header
        setTotalBytes(newData, newTotal);
        int numElem = getNumElements(newData);
        if (numElem != LP_HDR_NUMELE_UNKNOWN) {
            if (where != LP_REPLACE || isDelete) {
                if (!isDelete) {
                    numElem++;
                } else {
                    numElem--;
                }
                setNumElements(newData, numElem);
            }
        }

        return new ListPack(newData);
    }

    /**
     * Insert a string element that must be stored as string encoding (not integer).
     */
    private ListPack doInsertString(byte[] ele, int pos, int where) {
        if (where == LP_AFTER) {
            pos = pos + currentEncodedSizeUnsafe(pos) + (int) encodeBacklenBytes(currentEncodedSizeUnsafe(pos));
            where = LP_BEFORE;
        }

        // Compute string encoding bytes
        byte[] strEnc = encodeString(ele);
        int encLen = strEnc.length;

        int backlenSize = (int) encodeBacklen(null, encLen);
        byte[] backlenBuf = new byte[5];
        encodeBacklen(backlenBuf, encLen);

        int oldTotal = getTotalBytes(data);
        int replacedLen = 0;
        if (where == LP_REPLACE) {
            replacedLen = currentEncodedSizeUnsafe(pos);
            replacedLen += (int) encodeBacklenBytes(replacedLen);
        }

        int newTotal = oldTotal + encLen + backlenSize - replacedLen;
        byte[] newData = new byte[newTotal];

        if (where == LP_BEFORE) {
            System.arraycopy(data, 0, newData, 0, pos);
            System.arraycopy(strEnc, 0, newData, pos, encLen);
            System.arraycopy(backlenBuf, 0, newData, pos + encLen, backlenSize);
            System.arraycopy(data, pos, newData, pos + encLen + backlenSize, oldTotal - pos);
        } else { // LP_REPLACE
            System.arraycopy(data, 0, newData, 0, pos);
            System.arraycopy(strEnc, 0, newData, pos, encLen);
            System.arraycopy(backlenBuf, 0, newData, pos + encLen, backlenSize);
            int srcStart = pos + replacedLen;
            int dstStart = pos + encLen + backlenSize;
            System.arraycopy(data, srcStart, newData, dstStart, oldTotal - srcStart);
        }

        setTotalBytes(newData, newTotal);
        int numElem = getNumElements(newData);
        if (numElem != LP_HDR_NUMELE_UNKNOWN) {
            if (where != LP_REPLACE) {
                numElem++;
                setNumElements(newData, numElem);
            }
        }

        return new ListPack(newData);
    }

    // =========================================================
    // Internal: encoding helpers
    // =========================================================

    /**
     * Encode integer value into intenc buffer. Returns encoding length in enclen[0].
     * Mirrors lpEncodeIntegerGetType from Redis.
     */
    private static void encodeIntegerGetType(long v, byte[] intenc, long[] enclen) {
        if (v >= 0 && v <= 127) {
            if (intenc != null) intenc[0] = (byte) v;
            if (enclen != null) enclen[0] = 1;
        } else if (v >= -4096 && v <= 4095) {
            long uv = v < 0 ? ((long) 1 << 13) + v : v;
            if (intenc != null) {
                intenc[0] = (byte) ((uv >> 8) | LP_ENCODING_13BIT_INT);
                intenc[1] = (byte) (uv & 0xFF);
            }
            if (enclen != null) enclen[0] = 2;
        } else if (v >= -32768 && v <= 32767) {
            long uv = v < 0 ? ((long) 1 << 16) + v : v;
            if (intenc != null) {
                intenc[0] = (byte) LP_ENCODING_16BIT_INT;
                intenc[1] = (byte) (uv & 0xFF);
                intenc[2] = (byte) (uv >> 8);
            }
            if (enclen != null) enclen[0] = 3;
        } else if (v >= -8388608 && v <= 8388607) {
            long uv = v < 0 ? ((long) 1 << 24) + v : v;
            if (intenc != null) {
                intenc[0] = (byte) LP_ENCODING_24BIT_INT;
                intenc[1] = (byte) (uv & 0xFF);
                intenc[2] = (byte) ((uv >> 8) & 0xFF);
                intenc[3] = (byte) (uv >> 16);
            }
            if (enclen != null) enclen[0] = 4;
        } else if (v >= -2147483648L && v <= 2147483647L) {
            long uv = v < 0 ? ((long) 1 << 32) + v : v;
            if (intenc != null) {
                intenc[0] = (byte) LP_ENCODING_32BIT_INT;
                intenc[1] = (byte) (uv & 0xFF);
                intenc[2] = (byte) ((uv >> 8) & 0xFF);
                intenc[3] = (byte) ((uv >> 16) & 0xFF);
                intenc[4] = (byte) (uv >> 24);
            }
            if (enclen != null) enclen[0] = 5;
        } else {
            // 64-bit
            long uv = v; // reinterpret as unsigned via bit pattern
            if (intenc != null) {
                intenc[0] = (byte) LP_ENCODING_64BIT_INT;
                intenc[1] = (byte) (uv & 0xFF);
                intenc[2] = (byte) ((uv >> 8) & 0xFF);
                intenc[3] = (byte) ((uv >> 16) & 0xFF);
                intenc[4] = (byte) ((uv >> 24) & 0xFF);
                intenc[5] = (byte) ((uv >> 32) & 0xFF);
                intenc[6] = (byte) ((uv >> 40) & 0xFF);
                intenc[7] = (byte) ((uv >> 48) & 0xFF);
                intenc[8] = (byte) (uv >> 56);
            }
            if (enclen != null) enclen[0] = 9;
        }
    }

    /**
     * Determine encoding type for a string element.
     * Returns 0 (LP_ENCODING_INT) if integer encoding is possible (intenc/enclen populated),
     * or 1 (LP_ENCODING_STRING) otherwise (enclen populated with string encoding length).
     */
    private static int encodeGetType(byte[] ele, int size, byte[] intenc, long[] enclen) {
        long[] v = new long[1];
        if (lpStringToInt64(ele, size, v)) {
            encodeIntegerGetType(v[0], intenc, enclen);
            return 0; // LP_ENCODING_INT
        } else {
            if (size < 64) enclen[0] = 1 + size;
            else if (size < 4096) enclen[0] = 2 + size;
            else enclen[0] = 5 + (long) size;
            return 1; // LP_ENCODING_STRING
        }
    }

    /**
     * Encode a string element into its binary representation (encoding + data bytes).
     * Mirrors lpEncodeString from Redis.
     */
    private static byte[] encodeString(byte[] s) {
        int len = s.length;
        if (len < 64) {
            byte[] buf = new byte[1 + len];
            buf[0] = (byte) (len | LP_ENCODING_6BIT_STR);
            System.arraycopy(s, 0, buf, 1, len);
            return buf;
        } else if (len < 4096) {
            byte[] buf = new byte[2 + len];
            buf[0] = (byte) ((len >> 8) | LP_ENCODING_12BIT_STR);
            buf[1] = (byte) (len & 0xFF);
            System.arraycopy(s, 0, buf, 2, len);
            return buf;
        } else {
            byte[] buf = new byte[5 + len];
            buf[0] = (byte) LP_ENCODING_32BIT_STR;
            buf[1] = (byte) (len & 0xFF);
            buf[2] = (byte) ((len >> 8) & 0xFF);
            buf[3] = (byte) ((len >> 16) & 0xFF);
            buf[4] = (byte) ((len >> 24) & 0xFF);
            System.arraycopy(s, 0, buf, 5, len);
            return buf;
        }
    }

    // =========================================================
    // Internal: backlen encoding/decoding
    // =========================================================

    /**
     * Encode backlen into buf (or just return size if buf is null).
     * Mirrors lpEncodeBacklen from Redis.
     * The backlen is written with the MOST significant byte first (buf[0]),
     * with continuation bit 0x80 set on all bytes EXCEPT the last.
     */
    private static long encodeBacklen(byte[] buf, long l) {
        if (l <= 127) {
            if (buf != null) buf[0] = (byte) l;
            return 1;
        } else if (l < 16383) {
            if (buf != null) {
                buf[0] = (byte) (l >> 7);
                buf[1] = (byte) ((l & 127) | 128);
            }
            return 2;
        } else if (l < 2097151) {
            if (buf != null) {
                buf[0] = (byte) (l >> 14);
                buf[1] = (byte) (((l >> 7) & 127) | 128);
                buf[2] = (byte) ((l & 127) | 128);
            }
            return 3;
        } else if (l < 268435455) {
            if (buf != null) {
                buf[0] = (byte) (l >> 21);
                buf[1] = (byte) (((l >> 14) & 127) | 128);
                buf[2] = (byte) (((l >> 7) & 127) | 128);
                buf[3] = (byte) ((l & 127) | 128);
            }
            return 4;
        } else {
            if (buf != null) {
                buf[0] = (byte) (l >> 28);
                buf[1] = (byte) (((l >> 21) & 127) | 128);
                buf[2] = (byte) (((l >> 14) & 127) | 128);
                buf[3] = (byte) (((l >> 7) & 127) | 128);
                buf[4] = (byte) ((l & 127) | 128);
            }
            return 5;
        }
    }

    /** Returns only the number of bytes needed for backlen encoding. */
    private static long encodeBacklenBytes(long l) {
        return encodeBacklen(null, l);
    }

    /**
     * Decode backlen starting from byte at index `end` (the last byte of the backlen field).
     * The backlen field is stored with the most-significant byte first in memory,
     * and the last byte (at `end`) has its 0x80 bit CLEAR (it's the terminating byte).
     * Bytes before it (at lower indices) have 0x80 SET.
     *
     * Mirrors lpDecodeBacklen from Redis (which reads backwards from a pointer).
     *
     * In Redis C:
     *   p points to the last byte of the entry (the last backlen byte).
     *   p[0] is the last byte (LSB side), p[-1], p[-2]... are higher bytes.
     *   The last byte (lowest address in the backlen sequence from the start) has bit7=0.
     *   All preceding bytes have bit7=1.
     *
     * Wait — let me re-read the Redis code carefully:
     *
     *   lpEncodeBacklen writes:
     *     buf[0] = l>>7 (MSB part, no continuation bit)
     *     buf[1] = (l&127)|128 (LSB part, continuation bit SET)
     *
     *   lpDecodeBacklen reads backwards from p (end of entry):
     *     if !(p[0] & 128): return p[0] & 127  (single byte, p[0] is the last byte)
     *     val = p[0] & 127
     *     if !(p[-1] & 128): return val | (p[-1] & 127) << 7
     *
     * So in the stored layout: [buf[0]=MSB_no_cont, buf[1]=LSB_with_cont]
     * When reading backwards from the last byte (buf[1]):
     *   p[0] = buf[1] = (l&127)|128  -> has bit7 set -> continue
     *   p[-1] = buf[0] = l>>7        -> no bit7 -> stop
     *   val = (buf[1] & 127) | ((buf[0] & 127) << 7)
     *       = (l & 127) | ((l >> 7) << 7) = l  ✓
     *
     * So `end` is the index of the LAST byte of the backlen field (buf[N-1]).
     */
    private long decodeBacklen(int end) {
        // p[0] = data[end], p[-1] = data[end-1], etc.
        long val;
        int b0 = data[end] & 0xFF;

        if ((b0 & 128) == 0) {
            // Single byte
            return b0 & 127;
        }

        // Two bytes
        val = b0 & 127L;
        int b1 = data[end - 1] & 0xFF;
        if ((b1 & 128) == 0) {
            return val | ((b1 & 127L) << 7);
        }

        // Three bytes
        val |= (b1 & 127L) << 7;
        int b2 = data[end - 2] & 0xFF;
        if ((b2 & 128) == 0) {
            return val | ((b2 & 127L) << 14);
        }

        // Four bytes
        val |= (b2 & 127L) << 14;
        int b3 = data[end - 3] & 0xFF;
        if ((b3 & 128) == 0) {
            return val | ((b3 & 127L) << 21);
        }

        // Five bytes
        val |= (b3 & 127L) << 21;
        int b4 = data[end - 4] & 0xFF;
        if ((b4 & 128) == 0) {
            return val | ((b4 & 127L) << 28);
        }

        // Invalid
        throw new IllegalStateException("Invalid backlen encoding at " + end);
    }

    // =========================================================
    // Internal: two's complement conversions
    // =========================================================

    private static long toSigned13(int uval) {
        // negstart = 1<<12 = 4096, negmax = 8191
        if (uval >= 4096) {
            long v = 8191L - uval;
            return -v - 1;
        }
        return uval;
    }

    private static long toSigned16(int uval) {
        // negstart = 1<<15 = 32768, negmax = 65535
        if (uval >= 32768) {
            long v = 65535L - uval;
            return -v - 1;
        }
        return uval;
    }

    private static long toSigned24(int uval) {
        // negstart = 1<<23 = 8388608, negmax = 16777215
        if (uval >= 8388608) {
            long v = 16777215L - uval;
            return -v - 1;
        }
        return uval;
    }

    private static long toSigned32(long uval) {
        // negstart = 1<<31 = 2147483648, negmax = 4294967295
        if (uval >= 2147483648L) {
            long v = 4294967295L - uval;
            return -v - 1;
        }
        return uval;
    }

    // =========================================================
    // Internal: encoding type predicates
    // =========================================================

    private static boolean is7BitUint(int enc) {
        return (enc & LP_ENCODING_7BIT_UINT_MASK) == LP_ENCODING_7BIT_UINT;
    }

    private static boolean is6BitStr(int enc) {
        return (enc & LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR;
    }

    private static boolean is13BitInt(int enc) {
        return (enc & LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT;
    }

    private static boolean is12BitStr(int enc) {
        return (enc & LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR;
    }

    private static boolean is16BitInt(int enc) {
        return enc == LP_ENCODING_16BIT_INT;
    }

    private static boolean is24BitInt(int enc) {
        return enc == LP_ENCODING_24BIT_INT;
    }

    private static boolean is32BitInt(int enc) {
        return enc == LP_ENCODING_32BIT_INT;
    }

    private static boolean is64BitInt(int enc) {
        return enc == LP_ENCODING_64BIT_INT;
    }

    private static boolean is32BitStr(int enc) {
        return enc == LP_ENCODING_32BIT_STR;
    }

    // =========================================================
    // Internal: header read/write helpers
    // =========================================================

    private static int getTotalBytes(byte[] d) {
        return (d[0] & 0xFF)
                | ((d[1] & 0xFF) << 8)
                | ((d[2] & 0xFF) << 16)
                | ((d[3] & 0xFF) << 24);
    }

    private static void setTotalBytes(byte[] d, int v) {
        d[0] = (byte) (v & 0xFF);
        d[1] = (byte) ((v >> 8) & 0xFF);
        d[2] = (byte) ((v >> 16) & 0xFF);
        d[3] = (byte) ((v >> 24) & 0xFF);
    }

    private static int getNumElements(byte[] d) {
        return (d[4] & 0xFF) | ((d[5] & 0xFF) << 8);
    }

    private static void setNumElements(byte[] d, int v) {
        d[4] = (byte) (v & 0xFF);
        d[5] = (byte) ((v >> 8) & 0xFF);
    }

    // =========================================================
    // Internal: lpStringToInt64 — strict integer parsing
    // =========================================================

    /**
     * Convert a byte array to a signed 64-bit integer.
     * Mirrors Redis's lpStringToInt64: strict, no leading zeros, no spaces.
     *
     * @param s      byte array to parse
     * @param slen   length to parse
     * @param value  output array of length 1 for the result
     * @return true if successfully parsed
     */
    static boolean lpStringToInt64(byte[] s, int slen, long[] value) {
        if (slen == 0 || slen >= LONG_STR_SIZE) return false;

        int p = 0;
        boolean negative = false;
        long v;

        // Special case: single "0"
        if (slen == 1 && s[0] == '0') {
            if (value != null) value[0] = 0;
            return true;
        }

        if (s[p] == '-') {
            negative = true;
            p++;
            if (p == slen) return false; // only "-"
        }

        // First digit must be 1-9
        if (s[p] < '1' || s[p] > '9') return false;
        v = s[p] - '0';
        p++;

        while (p < slen) {
            if (s[p] < '0' || s[p] > '9') return false;
            // Overflow check
            if (v > Long.divideUnsigned(-1L, 10)) return false; // UINT64_MAX / 10
            v *= 10;
            int digit = s[p] - '0';
            if (Long.compareUnsigned(v, -1L - digit) > 0) return false; // v > UINT64_MAX - digit
            v += digit;
            p++;
        }

        if (negative) {
            // Check: v <= ((uint64_t)(-(INT64_MIN+1))+1) = 9223372036854775808
            // = Long.MIN_VALUE as unsigned = 0x8000000000000000
            // Using Long.MIN_VALUE as unsigned representation of 2^63
            if (Long.compareUnsigned(v, Long.MIN_VALUE) > 0) return false;
            if (value != null) value[0] = -v;
        } else {
            if (Long.compareUnsigned(v, Long.MAX_VALUE) > 0) return false;
            if (value != null) value[0] = v;
        }
        return true;
    }

    // =========================================================
    // Override
    // =========================================================

    @Override
    public String toString() {
        return "ListPack{size=" + size() + ", totalBytes=" + totalBytes() + "}";
    }

    // =========================================================
    // Iterator
    // =========================================================

    /**
     * Iterator over ListPack entries, supporting both forward and reverse traversal.
     */
    public static final class ListPackIterator implements Iterator<byte[]> {

        private final ListPack lp;
        private final boolean reverse;
        private int currentPos;
        private boolean started;

        ListPackIterator(ListPack lp, boolean reverse) {
            this.lp = lp;
            this.reverse = reverse;
            this.started = false;
            if (!reverse) {
                this.currentPos = lp.firstOffset();
            } else {
                this.currentPos = lp.lastOffset();
            }
        }

        @Override
        public boolean hasNext() {
            return currentPos != -1;
        }

        @Override
        public byte[] next() {
            if (currentPos == -1) throw new NoSuchElementException();
            byte[] val = lp.getAt(currentPos);
            if (!reverse) {
                currentPos = lp.nextOffset(currentPos);
            } else {
                currentPos = lp.prevOffset(currentPos);
            }
            return val;
        }
    }
}
