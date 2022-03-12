package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 适用于原生memtable或冷热表的记录
 */
public class Record {
    /**
     * 键
     */
    public Slice userKey;
    /**
     * 值
     */
    public Slice value;
    /**
     * 操作类型
     */
    public ValueType valueType;
    /**
     * 序列号
     */
    public long sequence;

    /**
     * 构造一个记录
     * @param userKey 键
     * @param value 值
     * @param valueType 操作类型
     * @param sequence 序列号
     */
    public Record(Slice userKey, Slice value, ValueType valueType, long sequence) {
        requireNonNull(userKey, "userKey is null");
        requireNonNull(value, "value is null");
        checkArgument(sequence >= 0, "sequenceNumber is negative");
        this.userKey = userKey;
        this.value = value;
        this.valueType = valueType;
        this.sequence = sequence;
    }
}
