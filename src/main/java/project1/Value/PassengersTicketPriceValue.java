package project1.Value;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PassengersTicketPriceValue implements WritableComparable<PassengersTicketPriceValue> {
    private IntWritable passengersCount;
    private DoubleWritable ticketPrice;

    public PassengersTicketPriceValue(Integer  passengersCount, Double ticketPrice) {
        this.passengersCount = new IntWritable(passengersCount);
        this.ticketPrice = new DoubleWritable(ticketPrice);
    }

    public IntWritable getPassengersCount() { return passengersCount; }
    public DoubleWritable getTicketPrice() { return ticketPrice; }

    @Override
    public void write(DataOutput out) throws IOException {
        passengersCount.write(out);
        ticketPrice.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        passengersCount.readFields(in);
        ticketPrice.readFields(in);
    }

    @Override
    public int compareTo(PassengersTicketPriceValue o) {
        int cmp = passengersCount.compareTo(o.passengersCount);
        if (cmp != 0) return cmp;
        return ticketPrice.compareTo(o.ticketPrice);
    }

    @Override
    public int hashCode() {
        return passengersCount.hashCode() * 163 + ticketPrice.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PassengersTicketPriceValue other) {
            return passengersCount.equals(other.passengersCount) && ticketPrice.equals(other.ticketPrice);
        }
        return false;
    }
}