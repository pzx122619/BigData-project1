package project1.Value;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PassengersTicketPriceValue implements WritableComparable<PassengersTicketPriceValue> {
    private IntWritable passengersCount;
    private DoubleWritable ticketPrice;
    private IntWritable tripsCount;

    public PassengersTicketPriceValue(Integer  passengersCount, Double ticketPrice) {
        set(passengersCount, ticketPrice, 1);
    }

    public PassengersTicketPriceValue(Integer  passengersCount, Double ticketPrice, Integer tripsCount) {
        set(passengersCount, ticketPrice, tripsCount);
    }

    public PassengersTicketPriceValue() {
        set(0, 0.0, 0);
    }

    public void set(Integer passengersCount, Double ticketPrice, Integer tripsCount) {
        this.passengersCount = new IntWritable(passengersCount);
        this.ticketPrice = new DoubleWritable(ticketPrice);
        this.tripsCount = new IntWritable(tripsCount);
    }

    public void sum(PassengersTicketPriceValue v) {
        set(
                this.passengersCount.get() + v.getPassengersCount().get(),
                this.ticketPrice.get() + v.getTicketPrice().get(),
                this.tripsCount.get() + v.getTripsCount().get()
        );

    }


    public IntWritable getPassengersCount() { return passengersCount; }
    public IntWritable getTripsCount() { return tripsCount; }
    public DoubleWritable getTicketPrice() { return ticketPrice; }

    @Override
    public void write(DataOutput out) throws IOException {
        passengersCount.write(out);
        tripsCount.write(out);
        ticketPrice.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        passengersCount.readFields(in);
        tripsCount.readFields(in);
        ticketPrice.readFields(in);
    }

    @Override
    public int compareTo(PassengersTicketPriceValue o) {
        int cmp = passengersCount.compareTo(o.passengersCount);
        if (cmp != 0) return cmp;
        cmp = ticketPrice.compareTo(o.ticketPrice);
        if (cmp != 0) return cmp;
        return tripsCount.compareTo(o.tripsCount);
    }

    @Override
    public int hashCode() {
        return passengersCount.hashCode() * 163 + ticketPrice.hashCode() + tripsCount.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PassengersTicketPriceValue other) {
            return passengersCount.equals(other.passengersCount) && ticketPrice.equals(other.ticketPrice) && tripsCount.equals(other.tripsCount);
        }
        return false;
    }
}