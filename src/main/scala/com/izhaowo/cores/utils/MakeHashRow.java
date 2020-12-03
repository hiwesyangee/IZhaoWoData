package com.izhaowo.cores.utils;

/**
 * 唯一化rowkey方法，重写HashCode，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class MakeHashRow {
    static final long[] byteTable = createLookupTable();
    static final long HSTART = 0xBB40E64DA205B064L;
    static final long HMULT = 7664345821815920749L;

    private static final long[] createLookupTable() {
        long[] byteTable = new long[256];
        long h = 0x544B2FBACAAF1684L;
        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < 31; j++) {
                h = (h >>> 7) ^ h;
                h = (h << 11) ^ h;
                h = (h >>> 10) ^ h;
            }
            byteTable[i] = h;
        }
        return byteTable;
    }

    public static long hashCode(CharSequence cs) {
        long h = HSTART;
        final long hmult = HMULT;
        final long[] ht = byteTable;
        final int len = cs.length();
        for (int i = 0; i < len; i++) {
            char ch = cs.charAt(i);
            h = (h * hmult) ^ ht[ch & 0xff];
            h = (h * hmult) ^ ht[(ch >>> 8) & 0xff];
        }
        return h;
    }
}
