package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.Slice;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * 冷热memtable
 * user key -> {value, seq, value type} (internal val)
 */
public class HCMemTable implements SeekingIterable<Slice, InternalVal> {
    /**
     * 跳表容器
     */
    private ConcurrentSkipListMap<Slice, InternalVal> table;
    /**
     * 表目前的大小
     */
    public AtomicLong approximateMemoryUsage = new AtomicLong();

    /**
     * 构造冷热表
     * @param userKeyComparator user key的比较器（给跳表用）
     */
    public HCMemTable(UserKeyComparator userKeyComparator) {
        requireNonNull(userKeyComparator, "userKeyComparator is null");
        table = new ConcurrentSkipListMap<>(userKeyComparator);
    }

    /**
     * 【废弃】返回迭代器
     * @return 冷热表的迭代器
     */
    @Override
    public SeekingIterator<Slice, InternalVal> iterator() {
        return null;
    }

    // region 外部接口

    /**
     * 向冷热表里增添一条没有的record，或是变更一条已经有的record（Key相同
     * Record形式的接口
     * @param record 待插入的记录
     */
    public void PutRecord(Record record) {
        requireNonNull(record, "record is null");
        // parse
        Slice userKey = record.userKey;
        Slice value = record.value;
        ValueType valueType = record.valueType;
        long sequence = record.sequence;
        // InternalVal
        InternalVal internalVal = new InternalVal(value, sequence, valueType);
        // 跳表里如果有，把之前的删掉
        if (table.containsKey(userKey)) {
            // 先取出之前的数据计算占用的内存大小，以能维护整个表的大小
            InternalVal oldItvVal = table.get(userKey);
            // 从表里删除
            table.remove(userKey);
            // 同步维护表大小
            SyncDel(new Record(userKey, oldItvVal.value, null, 0));
        }
        // 至此，跳表里一定没有这条记录，插入跳表中
        table.put(userKey, internalVal);
        // 同步维护表大小
        SyncAdd(record);
    }

    /**
     * 向冷热表里增添一条没有的record，或是变更一条已经有的record（Key相同
     * Entry形式的接口
     * @param entry 待插入的记录
     */
    public void PutEntry(Map.Entry<Slice, InternalVal> entry) {
        requireNonNull(entry, "entry is null");
        Record record = new Record(
                entry.getKey(),
                entry.getValue().value,
                entry.getValue().valueType,
                entry.getValue().sequenceNumber
        );
        PutRecord(record);
    }

    /**
     * “表溢” for 冷热表
     * 作为冷热表，达到了“溢出”，例如需要拆分冷热
     * @return 是否处在表溢出状态
     */
    public boolean IsOverflowForHotCold() {
        return approximateMemoryUsage.longValue() > HCOptions.hotColdBreakBufferSize;
    }

    /**
     * “表溢” for 热表
     * 作为热表，达到了“溢出”，例如需要拆分成两个热表
     * @return 是否处在表溢出状态
     */
    public boolean IsOverflowForHot() {
        return approximateMemoryUsage.longValue() > HCOptions.hotBreakBufferSize;
    }

    /**
     * 判断冷热表为空
     * @return 是否为空
     */
    public boolean IsEmpty() {
        return table.isEmpty();
    }

    /**
     * 清空冷热表
     */
    public void Clear() {
        this.table = new ConcurrentSkipListMap<>();
        this.approximateMemoryUsage = new AtomicLong();
        System.gc();
    }

    /**
     * 检查一条记录是否存在
     * @param record 要检查的记录
     * @return 是否存在
     */
    public boolean HasRecord(Record record) {
        requireNonNull(record, "record is null");
        return this.table.containsKey(record.userKey);
    }

    /**
     * 获取最大key，如果表空上报NoSuchElementException
     *
     * @return 最大key
     */
    public Slice MaxKey() {
        return table.lastKey();
    }

    /**
     * 获取最小key，如果表空上报NoSuchElementException
     *
     * @return 最小key
     */
    public Slice MinKey() {
        return table.firstKey();
    }

    /**
     * 是否有能力在表中增加此条目，而不引起表溢出
     *
     * @param entry 要添加的条目
     * @return 是否能添加
     */
    public boolean BeAbleToAddEntry(Map.Entry<Slice, InternalVal> entry) {
        requireNonNull(entry, "entry is null");
        return approximateMemoryUsage.longValue() +
                entry.getKey().length() +
                SIZE_OF_LONG +
                entry.getValue().value.length()
                <= HCOptions.hotBreakBufferSize;
    }

    /**
     * 弹出第一个记录，由调用方保证有元素可弹
     * @return 弹出的entry
     */
    public Map.Entry<Slice, InternalVal> PollFirstEntry() {
        // 弹出一个
        Map.Entry<Slice, InternalVal> entry = table.pollFirstEntry();
        // 同步维护表大小
        SyncDel(entry);
        // 返回弹出的元素
        return entry;
    }

    /**
     * 转换成队列，本地表将被摧毁
     *
     * @return 转换后的队列
     */
    public ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>> ToQueueAndDestroySelf() {
        ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>> res = new ConcurrentLinkedQueue<>();
        while (!table.isEmpty()) {
            res.offer(table.pollFirstEntry());
        }
        this.approximateMemoryUsage.set(0); // 已经destroy掉了，剩下0大小
        return res;
    }

    // endregion 外部接口

    // region 私有工具

    /**
     * 添加元素时同步表大小
     * @param record 要添加的记录
     */
    public void SyncAdd(Record record) {
        requireNonNull(record, "record is null");
        Slice userKey = record.userKey;
        Slice value = record.value;
        this.approximateMemoryUsage.addAndGet(userKey.length() + SIZE_OF_LONG + value.length());
    }

    /**
     * 添加元素时同步表大小
     * @param entry 要添加的记录
     */
    public void SyncAdd(Map.Entry<Slice, InternalVal> entry) {
        requireNonNull(entry, "entry is null");
        Slice userKey = entry.getKey();
        Slice value = entry.getValue().value;
        this.approximateMemoryUsage.addAndGet(userKey.length() + SIZE_OF_LONG + value.length());
    }

    /**
     * 删除元素时同步表大小
     * @param record 要添加的记录
     */
    public void SyncDel(Record record) {
        requireNonNull(record, "record is null");
        Slice userKey = record.userKey;
        Slice value = record.value;
        this.approximateMemoryUsage.addAndGet(-userKey.length() - SIZE_OF_LONG - value.length());
    }

    /**
     * 删除元素时同步表大小
     * @param entry 要添加的记录
     */
    public void SyncDel(Map.Entry<Slice, InternalVal> entry) {
        requireNonNull(entry, "entry is null");
        Slice userKey = entry.getKey();
        Slice value = entry.getValue().value;
        this.approximateMemoryUsage.addAndGet(-userKey.length() - SIZE_OF_LONG - value.length());
    }

    // endregion 私有工具
}
