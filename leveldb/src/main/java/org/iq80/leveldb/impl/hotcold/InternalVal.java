package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 冷热表（HCMemtable）的Val
 */
public class InternalVal {
    /**
     * LevelDB 记录-value
     */
    public final Slice value;
    /**
     * LevelDB 记录-序列号
     */
    public final long sequenceNumber;
    /**
     * LevelDB 记录-操作类型
     */
    public final ValueType valueType;

    /**
     * 构造一个InternalVal
     * @param value value
     * @param sequenceNumber 序列号
     * @param valueType 操作类型
     */
    public InternalVal(Slice value, long sequenceNumber, ValueType valueType) {
        requireNonNull(value, "value is null");
        checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        requireNonNull(valueType, "valueType is null");
        this.value = value;
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }
}
