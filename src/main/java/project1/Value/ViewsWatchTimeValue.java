package project1.Value;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ViewsWatchTimeValue implements WritableComparable<ViewsWatchTimeValue> {
    private DoubleWritable watchTime;
    private IntWritable viewsCount;

    public ViewsWatchTimeValue(Double watchTime) {
        set(watchTime, 1);
    }

    public ViewsWatchTimeValue(Double watchTime, Integer viewsCount) {
        set(watchTime, viewsCount);
    }

    public ViewsWatchTimeValue() {
        set(0.0, 0);
    }

    public void set(Double watchTime, Integer viewsCount) {
        this.watchTime = new DoubleWritable(watchTime);
        this.viewsCount = new IntWritable(viewsCount);
    }


    public IntWritable getViewsCount() { return viewsCount; }
    public DoubleWritable getWatchTime() { return watchTime; }

    @Override
    public void write(DataOutput out) throws IOException {
        viewsCount.write(out);
        watchTime.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        viewsCount.readFields(in);
        watchTime.readFields(in);
    }

    @Override
    public int compareTo(ViewsWatchTimeValue o) {
        int cmp = watchTime.compareTo(o.watchTime);
        if (cmp != 0) return cmp;
        return viewsCount.compareTo(o.viewsCount);
    }

    @Override
    public int hashCode() {
        return watchTime.hashCode() * 163 + viewsCount.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ViewsWatchTimeValue other) {
            return  watchTime.equals(other.watchTime) && viewsCount.equals(other.viewsCount);
        }
        return false;
    }
}