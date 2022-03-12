package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.util.Slice;

import javax.swing.plaf.IconUIResource;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Objects.requireNonNull;

/**
 * 分层热表
 * ([0, 0])
 * ([1, 0] [1, 1] [1, 2] [1, 3] [1, 4])
 * ([2, 0] [2, 1] [2, 2] [2, 3] [2, 4] [2, 5] [2, 6]...)
 * ... more levels ...
 */
public class HCLevelHot {
    /**
     * 每一层的层数
     */
    private final int[] levelSizes;
    /**
     * 每一层的热表
     */
    private final ArrayList<ArrayList<HCMemTable>> levels;
    /**
     * 比较器
     */
    private final UserKeyComparator userKeyComparator;

    // region 构造器

    /**
     * 构造时传入比较器，以及每一层的最大表数
     * @param userKeyComparator 比较器
     * @param levelSizes 每一层的最大表数
     */
    public HCLevelHot(UserKeyComparator userKeyComparator, int[] levelSizes) {
        requireNonNull(userKeyComparator, "userKeyComparator is null");
        requireNonNull(levelSizes, "levelSizes is null");
        // 第0级必须为1
        if (levelSizes[0] != 1) {
            throw new UnsupportedOperationException("The first level must have 1 hot table");
        }
        // 每一层的层数
        this.levelSizes = levelSizes;
        // 比较器
        this.userKeyComparator = userKeyComparator;
        // 创建每一层的层容器（先不放置任何表）
        this.levels = new ArrayList<>();
        for (int levelId = 0; levelId < levelSizes.length; levelId++) { // level id
            // int levelSize = levelSizes[levelId]; // level size
            levels.add(new ArrayList<>());
        }
        // 第0层唯一的表创建成空表
        this.levels.get(0).add(new HCMemTable(userKeyComparator));
        // 因此初始时表的结构为
        // ([0, 0])                 第0层
        // ()                       第1层
        // ()                       第2层
    }

    // endregion 构造器

    // region 外部访问接口

    /**
     * 检查root层中是否有userKey，如有就执行替换操作，并返回true，否则返回false
     * 替换发生时，所导致的分裂和向下合并也需要考虑到
     * @param record 要操作的记录
     * @return 是否执行了替换操作
     */
    public boolean CheckAndReplace_IN(Record record) {
        requireNonNull(record, "record is null");
        // 仅在有这条记录时候执行此操作
        if (this.Root().HasRecord(record)) {
            this.Root().PutRecord(record);
            // 替换完成可能导致拆表和合并，递归处理
            // 虽然该接口是IN的，但是由逻辑来决定操作，为了节省逻辑带来的性能消耗，这里判断下只有溢出时候做
            if (this.Root().IsOverflowForHot()) {
                this.SplitTableAndMergeCascade_IN(0, 0);
            }
            return true;
        }
        return false;
    }

    /**
     * 向冷热系统中加入热数据，并向下做规约化逻辑
     * @param records 要添加的热数据
     */
    public void PutRecords(LinkedList<Record> records) {
        requireNonNull(records, "records is null");
        // 对每条记录put进去
        for (Record r: records) {
            this.Root().PutRecord(r);
        }
        // put完成后如果需要，规整化一下
        if (this.Root().IsOverflowForHot()) {
            this.SplitTableAndMergeCascade_IN(0, 0);
        }
    }

    // endregion 外部访问接口

    // region 内部过程

    /**
     * 对于levelPos【位置】的【层】的tablePos【位置】的【热表】，作如下操作
     * 1. 如果表未满，结束，否则进到2
     * 2. 拆分表，成为两个表，加入到该层相应位置，如果该层不满，结束，否则进到3
     * 3. 如果该层是最后一层，进到4，否则进到5
     * 4（最后一层溢）. 把最末尾的表dump到磁盘，dump完成后，如果该层不溢，结束，否则回到4
     * 5（非最后一层溢）. 把最末尾的表向下递归merge，同样执行以上操作，merge后，如果该层不溢，结束，否则回到5
     * @param levelPos 待操作的层的位置
     * @param tablePos 待操作的表的位置
     */
    private void SplitTableAndMergeCascade_IN(int levelPos, int tablePos) {
        // [step 1,2] 如果表没满，什么都不需要做，否则拆分表，形成若干子表，并加入到相应位置
        this.SplitTableAndInsertToCurrentLevel_IN(levelPos, tablePos);
        // [step 2,3,4,5] 如果操作后该层不满，结束，否则根据是否是最后一层溢作不同处理
        while (IsOverflowLevel(levelPos)) { // [step 2,4 | 2,5]
            if (IsLastLevel(levelPos)) { // [step 3 branch A] 最后一层
                DumpTableAndReorgLevel(levelPos, -1);
            }
            else { // [step 3 branch B] 非最后一层
                MergeTableAndReorgLevel(levelPos, -1, levelPos + 1);
            }
        }
    }

    /**
     * 将某个热表拆分成若干个热表，并加入到相应位置
     * @param levelPos 表所在的level
     * @param tablePos 表所在的位置
     */
    private void SplitTableAndInsertToCurrentLevel_IN(int levelPos, int tablePos) {
        int lid = LID(levelPos), tid = TID(levelPos, tablePos);
        // 将该表从层中取出来
        HCMemTable table = levels.get(lid).get(tid);
        levels.get(lid).remove(tid);
        // 做拆分（如果需要）
        ArrayList<HCMemTable> splitRes = SplitTable_IN(table);
        // 遍历插入回这个层的相应位置去
        for (int i = 0; i < splitRes.size(); i ++ ) {
            levels.get(lid).add(tid + i, splitRes.get(i));
        }
    }

    /**
     * 将指定的表dump磁盘，并整理
     * @param levelPos 表所在的level
     * @param tablePos 表的位置
     */
    private void DumpTableAndReorgLevel(int levelPos, int tablePos) {
        int lid = LID(levelPos), tid = TID(levelPos, tablePos);
        HCMemTable table = levels.get(lid).get(tid);
        DumpTable(table); // dump disk
        levels.get(lid).remove(tid); // reorg cur level
    }

    /**
     * 将指定的表merge到某一层，并整理
     * @param levelPos 表所在的level
     * @param tablePos 表的位置
     * @param targetLevelPos 要合并到的表的层
     */
    private void MergeTableAndReorgLevel(int levelPos, int tablePos, int targetLevelPos) {
        int lid = LID(levelPos), tid = TID(levelPos, tablePos), tlid = LID(targetLevelPos);
        // 和自己这层没什么可合并的（目前的实现是和下一层合并，即总是有tlid == lid + 1）
        if (lid == tlid) return;
        // 取出合并的主体表
        HCMemTable table = levels.get(lid).get(tid);
        levels.get(lid).remove(tid); // reorg cur level
        // 取出要合并到的层
        ArrayList<HCMemTable> levelToMerge = levels.get(tlid);
        // 特殊情况：要合并到的层没有表，直接放到那一层上
        if (levelToMerge.isEmpty()) {
            levelToMerge.add(table);
            return;
        }
        // 根据当前表的最大和最小key，查找要合并的区间左右位置
        Slice curMinKey = table.MinKey();
        Slice curMaxKey = table.MaxKey();
        // 特殊情况：当前表最大key < 层中最左表的最小key，不相交且当前表插在最左边
        if (userKeyComparator.compare(curMaxKey, levelToMerge.get(0).MinKey()) < 0) {
            levelToMerge.add(0, table);
            // 借助上游接口递归reorg（实际上插入的表一定不需要split）
            SplitTableAndMergeCascade_IN(tlid, -1);
            return;
        }
        // 特殊情况：当前最小key > 层中最右表的最大key，不相交且当前表插在最右边
        int len = levelToMerge.size();
        if (userKeyComparator.compare(curMinKey, levelToMerge.get(len - 1).MaxKey()) > 0) {
            levelToMerge.add(table); // add to tail
            // 借助上游接口递归reorg（实际上插入的表一定不需要split）
            SplitTableAndMergeCascade_IN(tlid, -1);
            return;
        }
        // 一般情况：分别查找到左侧和右侧的位置，以在target level上构成合并区间 [LPos, RPos]
        int LPos = SearchLeftPos(curMinKey, levelToMerge, true);
        int RPos = SearchRightPos(curMaxKey, levelToMerge, true);
        // 将[LPos, RPos]区间里所有的表，联通当前表table一起进行规约成几个大的热表
        ArrayList<HCMemTable> tablesToReduce = new ArrayList<>((RPos - LPos + 1));
        for (int i = LPos; i <= RPos; i ++ ) {
            tablesToReduce.add(levelToMerge.get(i));
        }
        ArrayList<HCMemTable> reducedTables = ReduceOneTableToReducedTables(table, tablesToReduce);
        // 往前插LPos之前的表
        for (int i = LPos - 1; i >= 0; i -- ) {
            reducedTables.add(0, levelToMerge.get(i));
        }
        // 往后插RPos之后的表
        for (int i = RPos + 1; i < levelToMerge.size(); i ++ ) {
            reducedTables.add(levelToMerge.get(i));
        }
        // 直接替换掉tlid这一层的表
        levels.set(tlid, reducedTables);
        // 最后对tlid这一层做reorg
        // 借助上游接口递归reorg（实际上规约后的表一定不需要split）
        SplitTableAndMergeCascade_IN(tlid, -1);
    }

    /**
     * 【废弃】将指定的table列表规约成有序的几个热表，保证规约后是有序、不溢、无重复键的
     * @param tables 要规约的一系列热表，这些热表输入时并不保证有序、不溢、无重复键
     * @return 规约后的热表，一定是有序、不溢、无重复键的
     */
    public ArrayList<HCMemTable> ReduceTables(ArrayList<HCMemTable> tables) {
        requireNonNull(tables, "tables is null");
        // 将tables中的每个表转换为entry队列，以方便进行peek和poll操作
        ArrayList<ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>>> queues = new ArrayList<>(tables.size());
        for (int i = 0; i < tables.size(); i ++ ) {
            queues.add(tables.get(i).ToQueueAndDestroySelf());
        }
        // *至此tables中的每个热表已被destroy掉，不可用
        // 待返回初始容量可以和之前保持一致（这个值只影响capacity不影响实际size），避免内部data数组增长引起耗时
        ArrayList<HCMemTable> res = new ArrayList<>(tables.size());
        // 记录当前待插入的表
        HCMemTable curTable = new HCMemTable(userKeyComparator);
        // 记录上一次插入的元素，用于去重和去除无效条目
        Map.Entry<Slice, InternalVal> lastInsertEntry = null;
        // 不停地从queues中取出每个队列的队头元素规约到待插入表里
        // 规约的语义：加进来（A类），或是暂时按下不动（B类），或是判定无效删掉（C类）
        for ( ; ; ) {
            // 记录在queues选中的queue下标
            int selectedTable = -1;
            // 遍历每个queue
            for (int i = 0; i < queues.size(); i ++ ) {
                // 当前queue
                ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>> curQ = queues.get(i);
                // 当前条目
                Map.Entry<Slice, InternalVal> curEntry = curQ.peek();
                // 当前条目比上一次插入的旧（key相同）：判定无效（C类）
                if (lastInsertEntry != null && StaleCmp(curEntry, lastInsertEntry)) {

                }
                // 当前还没有选中：先选中当前的（B类）

                // 当前的比选中的key大：暂时跳过不考虑（B类）

                // 当前的比选中的key小：先选中当前的（B类）

                // 当前的和选中的一样大，但更新：先选中当前的（B类）

                // 当前的和选中的一样大，但更旧：判定无效（C类）
            }
            // 一轮过后没有选中，说明队列已经全空了，直接跳出
            if (selectedTable == -1) break;
            // 一轮过后，选中的queue的队头需要加到表中（A类）
            ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>> queue = queues.get(selectedTable);
            Map.Entry<Slice, InternalVal> entry = queue.poll();

            // 更新当前插入的条目
        }

        return res;
    }

    /**
     * 将一个热表规约到若干个规整的热表中，返回规约后的热表组
     * 当调用此方法时，由调用方保证oneTable中的每条记录在tables中都能找到一个要插入的表
     * @param oneTable 独立热表
     * @param tables 规约形式的热表组
     * @return 规约后的热表组
     */
    public ArrayList<HCMemTable> ReduceOneTableToReducedTables(HCMemTable oneTable, ArrayList<HCMemTable> tables) {
        requireNonNull(oneTable, "oneTable is null");
        requireNonNull(tables, "tables is null");
        // 待返回初始容量可以和之前保持一致（这个值只影响capacity不影响实际size），避免内部data数组增长引起耗时
        ArrayList<HCMemTable> res = new ArrayList<>(tables.size() + 1);
        // 将表中的每个key都二分找到一个规约位置，然后put进去即可
        // 由于调用方保证oneTable是来自上层的热表，所以相同key时数据一定比tables中的更新，所以直接put即可
        // 首先获取所有的entry
        ConcurrentLinkedQueue<Map.Entry<Slice, InternalVal>> entries = oneTable.ToQueueAndDestroySelf();
        // 然后每次取出一个，put到tables中
        while (!entries.isEmpty()) {
            // 取队头
            Map.Entry<Slice, InternalVal> t = entries.poll();
            // 找到插入位置
            int pos = SearchLeftPos(t.getKey(), tables, true);
            // 插入到相应table中
            tables.get(pos).PutEntry(t);
        }
        // 最后，每个表都可能会分裂，取每个表出来分裂加到res里
        for (HCMemTable t: tables){
            // 分裂操作
            ArrayList<HCMemTable> ts = SplitTable_IN(t);
            // 加到res中
            res.addAll(ts);
        }
        // 最后返回规约后的表组
        return res;
    }

    /**
     * 将一个热表分裂成规格化的表组（如果需要分裂）
     * 如果传入的table不需要分裂，那么会返回一个仅有一个table的表组
     * 分裂操作会不停取出表中的最小元素，一旦发现剩余元素不溢，那么剩余的表就是最后一个表
     * @param table 待分裂的热表
     * @return 分裂后的表组
     */
    public ArrayList<HCMemTable> SplitTable_IN(HCMemTable table) {
        requireNonNull(table, "table is null");
        // 待返回的表组
        ArrayList<HCMemTable> res = new ArrayList<>();
        // 要插入到的当前表
        HCMemTable curTable = new HCMemTable(userKeyComparator);
        // 只要表还是溢出的，就一直做
        while (table.IsOverflowForHot()) {
            // 弹出一个元素
            Map.Entry<Slice, InternalVal> t = table.PollFirstEntry();
            // 在插入之前如果curTable没地方插了要换个新table
            if (!curTable.BeAbleToAddEntry(t)) {
                res.add(curTable); // 换新的，要把旧的加到表组中
                curTable = new HCMemTable(userKeyComparator);
            }
            // 至此，一定有地方插，所以直接插进去
            curTable.PutEntry(t);
        }
        // 判断一下cur table里有数据就加进来（如果传入不溢的表会进入该逻辑中）
        if (!curTable.IsEmpty()) res.add(curTable);
        // 最后这个表剩余元素作为表组的最后一项，也要加进来
        res.add(table);
        return res;
    }

    // endregion 内部过程

    // region 内部工具

    /**
     * 获取第0层唯一的表
     * @return 根表
     */
    private HCMemTable Root() {
        return this.levels.get(0).get(0);
    }

    /**
     * 判断某一层是否溢了，注意正好等于层里表最大数的时候不算溢出
     * @param levelPos 要判断的层位置
     * @return 是否是溢出层
     */
    private boolean IsOverflowLevel(int levelPos) {
        int levelId = LID(levelPos);
        return levels.get(levelId).size() > levelSizes[levelId];
    }

    /**
     * level pos -> level id (LID)
     * 注意level pos和table pos是对实际数量取模滚动的，例如
     * levelPos为-1时表示获取最后一层，tablePos为-1时表示获取最后一个表
     * @param levelPos 层的位置
     * @return 层的id
     */
    private int LID(int levelPos) {
        int levelNum = levelSizes.length; // 总层数
        return (levelPos + levelNum) % levelNum;
    }

    /**
     * table pos -> table id (TID)
     * @param levelPos 层的位置
     * @param tablePos 表的位置
     * @return 表的id
     */
    private int TID(int levelPos, int tablePos) {
        int levelId = LID(levelPos);
        int levelTableNum = levels.get(levelId).size(); // 这一层有多少表
        return (tablePos + levelTableNum) % levelTableNum;
    }

    /**
     * 判断是否是最后一层
     * @param levelPos 层的位置
     * @return 是否是最后一层
     */
    private boolean IsLastLevel(int levelPos) {
        return LID(levelPos) == levelSizes.length - 1;
    }

    /**
     * 将某个热表dump磁盘
     * @param table 要dump磁盘的热表
     */
    private void DumpTable(HCMemTable table) {
        requireNonNull(table, "table is null");
        // todo
        System.out.println("=== Want to write disk ===");
        for (int i = 0; i < this.levels.size(); i ++ ) {
            System.out.print(this.levels.get(i).size());
            System.out.print(", ");
        }
        System.out.println("\n======================");
    }

    /**
     * 以target key为标，二分找到在给定的表行中落入的最左表位置，确保输入有解
     * @param tKey target key
     * @param tables 表行
     * @param useBiSearch 是否使用二分查找
     * @return 最左落入表位置
     */
    private int SearchLeftPos(Slice tKey, ArrayList<HCMemTable> tables, boolean useBiSearch) {
        requireNonNull(tKey, "tKey is null");
        requireNonNull(tables, "tables is null");
        int L = 0;
        // 二分找
        if (useBiSearch) {
            int R = tables.size() - 1;
            while (L < R) {
                int M = (L + R) >> 1;
                Slice tableMaxKey = tables.get(M).MaxKey();
                // target key <= table max key
                if (userKeyComparator.compare(tKey, tableMaxKey) <= 0)
                    R = M;
                else
                    L = M + 1;
            }
        }
        // 顺序找（从左往右）
        else {
            for ( ; L < tables.size(); L ++ ) {
                Slice tableMaxKey = tables.get(L).MaxKey();
                // target key <= table max key
                if (userKeyComparator.compare(tKey, tableMaxKey) <= 0)
                    break;
            }
        }
        return L;
    }

    /**
     * 以target key为标，二分找到在给定的表行中落入的最右侧表位置，确保输入有解
     * @param tKey target key
     * @param tables 表行
     * @param useBiSearch 是否使用二分查找
     * @return 最右落入表位置
     */
    private int SearchRightPos(Slice tKey, ArrayList<HCMemTable> tables, boolean useBiSearch) {
        requireNonNull(tKey, "tKey is null");
        requireNonNull(tables, "tables is null");
        int R = tables.size() - 1;
        // 二分找
        if (useBiSearch) {
            int L = 0;
            while (L < R) {
                int M = (L + R + 1) >> 1;
                Slice tableMinKey = tables.get(M).MinKey();
                // target key >= table min key
                if (userKeyComparator.compare(tKey, tableMinKey) >= 0)
                    L = M;
                else
                    R = M - 1;
            }
            R = L; // 二分结果为L，但后续返回的是R这个变量
        }
        // 顺序找（从右往左）
        else {
            for ( ; R >= 0; R -- ) {
                Slice tableMinKey = tables.get(R).MinKey();
                // target key >= table min key
                if (userKeyComparator.compare(tKey, tableMinKey) >= 0)
                    break;
            }
        }
        return R;
    }

    /**
     * 条目 a > b
     * @param a 条目a
     * @param b 条目b
     * @return 是否成立
     */
    public boolean GreaterCmp(Map.Entry<Slice, InternalVal> a, Map.Entry<Slice, InternalVal> b) {
        requireNonNull(a, "a is null");
        requireNonNull(b, "b is null");
        return userKeyComparator.compare(a.getKey(), b.getKey()) > 0;
    }

    /**
     * 条目 a < b
     * @param a 条目a
     * @param b 条目b
     * @return 是否成立
     */
    public boolean LessCmp(Map.Entry<Slice, InternalVal> a, Map.Entry<Slice, InternalVal> b) {
        requireNonNull(a, "a is null");
        requireNonNull(b, "b is null");
        return userKeyComparator.compare(a.getKey(), b.getKey()) < 0;
    }

    /**
     * a 新于 b
     * @param a 条目a
     * @param b 条目b
     * @return 是否成立
     */
    public boolean FreshCmp(Map.Entry<Slice, InternalVal> a, Map.Entry<Slice, InternalVal> b) {
        requireNonNull(a, "a is null");
        requireNonNull(b, "b is null");
        // key不同没有可比性
        if (userKeyComparator.compare(a.getKey(), b.getKey()) != 0) return false;
        return a.getValue().sequenceNumber > b.getValue().sequenceNumber;
    }

    /**
     * a 旧于 b
     * @param a 条目a
     * @param b 条目b
     * @return 是否成立
     */
    public boolean StaleCmp(Map.Entry<Slice, InternalVal> a, Map.Entry<Slice, InternalVal> b) {
        requireNonNull(a, "a is null");
        requireNonNull(b, "b is null");
        // key不同没有可比性
        if (userKeyComparator.compare(a.getKey(), b.getKey()) != 0) return false;
        return a.getValue().sequenceNumber < b.getValue().sequenceNumber;
    }

    // endregion 内部工具
}
