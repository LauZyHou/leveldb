package org.iq80.leveldb.impl.hotcold;

import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Slice;

import java.util.Comparator;

/**
 * user key的比较器
 */
public class UserKeyComparator implements Comparator<Slice> {
    private final UserComparator userComparator;

    public UserKeyComparator(UserComparator userComparator) {
        this.userComparator = userComparator;
    }

    public UserComparator getUserComparator()
    {
        return userComparator;
    }

    public String name()
    {
        return this.userComparator.name();
    }

    @Override
    public int compare(Slice o1, Slice o2) {
        return userComparator.compare(o1, o2);
    }
}
