package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.impl.hotcold.split.HotColdSpliter;
import org.iq80.leveldb.table.UserComparator;

import java.util.LinkedList;

import static java.util.Objects.requireNonNull;

/**
 * 冷热系统
 */
public class HCSys {
    /**
     * 冷热表
     */
    private final HCMemTable hcMemTable;
    /**
     * 其skip list比较器
     */
    private final UserKeyComparator userKeyComparator;
    /**
     * 冷热表的热度表
     */
    private final HeatTable heatTable;
    /**
     * 分层热表
     */
    private final HCLevelHot hcLevelHot;

    /**
     * 构造冷热系统实例
     * @param userComparator 冷热表内key比较器
     */
    public HCSys(UserComparator userComparator) {
        requireNonNull(userComparator, "userComparator is null");
        // 冷热表的skiplist比较器
        this.userKeyComparator = new UserKeyComparator(userComparator);
        // 冷热表
        this.hcMemTable = new HCMemTable(this.userKeyComparator);
        // 冷热表的热度表
        this.heatTable = new HeatTable();
        // 分层热表
        this.hcLevelHot = new HCLevelHot(this.userKeyComparator, new int[] {1, 5, 25});
    }

    /**
     * 向冷热系统中变更一条记录，语义：添加/修改/删除
     * 如果需要调用方刷盘，需要返回要刷盘的HCMemtable
     * @param record 要变更的记录
     * @return 要刷盘的记录
     */
    public LinkedList<Record> PutRecord(Record record) {
        requireNonNull(record, "record is null");
        // 冷数据容器，返回空表即不用刷盘
        LinkedList<Record> coldDataNeedToFlushDisk = new LinkedList<>();
        // 先检查热表第一级是否有这个key，有就直接替换了不用后续操作
        // 返回true表示成功替换掉
        if (this.hcLevelHot.CheckAndReplace_IN(record)) {
            return coldDataNeedToFlushDisk;
        }
        // 往全局冷热表里加入这条记录
        this.hcMemTable.PutRecord(record);
        // 热度+1
        this.heatTable.IncHeat(record.userKey);
        // 判断该操作后冷热表是否已经满了，如果满了就要分离冷热数据
        if (this.hcMemTable.IsOverflowForHotCold()) {
            // 准备好冷热数据容器
            LinkedList<Record> hotData = new LinkedList<>(); // 热
            // 调用拆分方法，填充冷热数据容器
            HotColdSpliter.func1(this.hcMemTable, this.heatTable, hotData, coldDataNeedToFlushDisk);
            // 清空冷热表
            this.hcMemTable.Clear();
            // 同步清空其热度表
            this.heatTable.Clear();
            // 热数据加入到分层系统中
            this.hcLevelHot.PutRecords(hotData);
        }
        return coldDataNeedToFlushDisk;
    }
}
