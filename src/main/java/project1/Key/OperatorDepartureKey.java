package project1.Key;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OperatorDepartureKey implements WritableComparable<OperatorDepartureKey> {
    Text operatorId;
    IntWritable departureHour;

    public OperatorDepartureKey() {
        set(
                "",
                0
        );
    }

    public OperatorDepartureKey(String  operatorId, Integer departureHour) {
        set(
                operatorId,
                departureHour
        );
    }

    public void set(String operatorId, Integer departureHour) {
        this.operatorId = new Text(operatorId);
        this.departureHour = new IntWritable(departureHour);
    }

    public Text getOperatorId() { return operatorId; }
    public IntWritable getDepartureHour() { return departureHour; }

    @Override
    public void write(DataOutput out) throws IOException {
        operatorId.write(out);
        departureHour.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        operatorId.readFields(in);
        departureHour.readFields(in);
    }

    @Override
    public int compareTo(OperatorDepartureKey o) {
        int cmp = operatorId.compareTo(o.operatorId);
        if (cmp != 0) return cmp;
        return departureHour.compareTo(o.departureHour);
    }

    @Override
    public int hashCode() {
        return operatorId.hashCode() * 163 + departureHour.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof OperatorDepartureKey other) {
            return operatorId.equals(other.operatorId) && departureHour.equals(other.departureHour);
        }
        return false;
    }

    @Override
    public String toString() {
        return operatorId+"\t"+departureHour;
    }
}