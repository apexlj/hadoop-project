package com.sq.historyTrackMapreduce.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 复合键：在排序阶段会据此复合键进行排序；先按照sim升序，再按照time升序进行二次排序
 *
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 16:30
 */
public class PairKey implements WritableComparable<PairKey> {

    private long sim;
    private long time;

    public void set(long sim, long time) {
        this.sim = sim;
        this.time = time;
    }

    public long getSim() {
        return sim;
    }

    public long getTime() {
        return time;
    }

    @Override
    public int compareTo(PairKey o) {
        if (sim != o.getSim()) {
            return (int) (sim - o.getSim());
        } else if (time != o.getTime()) {
            return (time > o.getTime()) ? 1 : -1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(sim);
        out.writeLong(time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sim = in.readLong();
        time = in.readLong();
    }

    @Override
    public int hashCode() {
        return String.valueOf(sim).hashCode() + String.valueOf(time).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PairKey)) return false;

        PairKey pairKey = (PairKey) o;

        return getSim() == pairKey.getSim() && getTime() == pairKey.getTime();
    }
}
