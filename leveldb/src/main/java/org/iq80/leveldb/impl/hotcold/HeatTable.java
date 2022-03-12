package org.iq80.leveldb.impl.hotcold;


import org.iq80.leveldb.util.Slice;

import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * 热度表
 */
public class HeatTable {
    /**
     * 存热度
     */
    private final ConcurrentHashMap<Slice, Integer> heatMap;

    // region 构造器

    /**
     * 构造热度表
     */
    public HeatTable() {
        heatMap = new ConcurrentHashMap<>();
    }

    // endregion 构造器

    // region 外部接口

    /**
     * 清空热度表
     */
    public void Clear() {
        heatMap.clear();
    }

    /**
     * 热度增1
     * @param key 要曾热度的key
     */
    public void IncHeat(Slice key) {
        requireNonNull(key, "key is null");
        if (heatMap.contains(key)) {
            Integer oldHeat = heatMap.get(key);
            heatMap.put(key, oldHeat + 1);
        }
        else {
            heatMap.put(key, 1);
        }
    }

    /**
     * 查询热度
     * @param key 要查询的key
     * @return 热度值
     */
    public int Get(Slice key) {
        requireNonNull(key, "key is null");
        if (heatMap.containsKey(key)) {
            return heatMap.get(key);
        }
        return 0; // 热度表中有的key热度至少为1，0表示无此key
    }

    // endregion 外部接口
}
