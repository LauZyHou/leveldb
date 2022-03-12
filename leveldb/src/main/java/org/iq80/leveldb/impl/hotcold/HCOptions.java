package org.iq80.leveldb.impl.hotcold;

/**
 * 冷热系统的配置（全局唯一）
 */
public class HCOptions {
    /**
     * 冷热拆分的表占用阈值
     */
    public static int hotColdBreakBufferSize = 4 << 20;
    /**
     * 热表的拆分阈值
     */
    public static int hotBreakBufferSize = 4 << 20;
}
