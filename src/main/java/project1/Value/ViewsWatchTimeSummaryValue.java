package project1.Value;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ViewsWatchTimeSummaryValue implements WritableComparable<ViewsWatchTimeSummaryValue> {

    DoubleWritable avgWatchTime;
    IntWritable totalViews;

    public ViewsWatchTimeSummaryValue() {
        set(0.0, 0);
    }

    public ViewsWatchTimeSummaryValue(Double avgWatchTime, Integer totalViews) {
        set(avgWatchTime, totalViews);
    }

    public void set(Double avgWatchTime, Integer totalViews) {
        this.avgWatchTime = new DoubleWritable(avgWatchTime);
        this.totalViews = new IntWritable(totalViews);
    }

    public DoubleWritable getAvgWatchTime() {
        return avgWatchTime;
    }

    public IntWritable getTotalViews() {
        return totalViews;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        avgWatchTime.write(dataOutput);
        totalViews.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        avgWatchTime.readFields(dataInput);
        totalViews.readFields(dataInput);
    }

    @Override
    public int compareTo(ViewsWatchTimeSummaryValue summary) {

        int comparison = avgWatchTime.compareTo(summary.avgWatchTime);

        if (comparison != 0) {
            return comparison;
        }

        return totalViews.compareTo(summary.totalViews);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ViewsWatchTimeSummaryValue summary = (ViewsWatchTimeSummaryValue) o;

        return totalViews.equals(summary.totalViews) && avgWatchTime.equals(summary.avgWatchTime);
    }

    @Override
    public int hashCode() {
        return avgWatchTime.hashCode() * 163 + totalViews.hashCode();
    }


    @Override
    public String toString() {
        return totalViews + "," + avgWatchTime;
    }
}