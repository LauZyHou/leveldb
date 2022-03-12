package org.iq80.leveldb.impl.hotcold.split;

import org.iq80.leveldb.impl.hotcold.HCMemTable;
import org.iq80.leveldb.impl.hotcold.HeatTable;
import org.iq80.leveldb.impl.hotcold.Record;

import java.util.LinkedList;

/**
 * 冷热拆分的方法集
 */
public class HotColdSpliter {
    /**
     * 取访问频率>2的数据中的10%作为热数据，其余为冷数据
     * @param hcMemTable 冷热表
     * @param heatTable 冷热表的热度表
     * @param hotData 返回的热数据集
     * @param coldData 返回的冷数据集
     */
    public static void func1(HCMemTable hcMemTable, HeatTable heatTable,
                             LinkedList<Record> hotData, LinkedList<Record> coldData) {
       // todo
    }
}
