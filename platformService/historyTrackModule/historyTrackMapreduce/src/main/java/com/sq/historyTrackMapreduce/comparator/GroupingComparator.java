package com.sq.historyTrackMapreduce.comparator;

import com.sq.historyTrackMapreduce.model.PairKey;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器：以sim卡号分组；在reduce端会据此对key进行分组
 *
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 16:44
 */
public class GroupingComparator implements RawComparator<PairKey> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
    }

    @Override
    public int compare(PairKey key1, PairKey key2) {
        return (int) (key1.getSim() - key2.getSim());
    }
}
