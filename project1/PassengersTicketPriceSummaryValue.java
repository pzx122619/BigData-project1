package project1.Value;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

public class PassengersTicketPriceSummaryValue implements WritableComparable<PassengersTicketPriceSummaryValue> {

    DoubleWritable avgTicketPrice;
    IntWritable totalPassengersCount;

    public PassengersTicketPriceSummaryValue() {
        set(0.0, 0);
    }

    public PassengersTicketPriceSummaryValue(Double avgTicketPrice, Integer totalPassengersCount) {
        set(avgTicketPrice, totalPassengersCount);
    }

    public void set(Double avgTicketPrice, Integer totalPassengersCount) {
        this.avgTicketPrice = new DoubleWritable(avgTicketPrice);
        this.totalPassengersCount = new IntWritable(totalPassengersCount);
    }

    public DoubleWritable getAvfTicketPrice() {
        return avgTicketPrice;
    }

    public IntWritable getTotalPassengersCount() {
        return totalPassengersCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        avgTicketPrice.write(dataOutput);
        totalPassengersCount.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        avgTicketPrice.readFields(dataInput);
        totalPassengersCount.readFields(dataInput);
    }

    @Override
    public int compareTo(PassengersTicketPriceSummaryValue summary) {

        int comparison = avgTicketPrice.compareTo(summary.avgTicketPrice);

        if (comparison != 0) {
            return comparison;
        }

        return totalPassengersCount.compareTo(summary.totalPassengersCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PassengersTicketPriceSummaryValue summary = (PassengersTicketPriceSummaryValue) o;

        return totalPassengersCount.equals(summary.totalPassengersCount) && avgTicketPrice.equals(summary.avgTicketPrice);
    }

    @Override
    public int hashCode() {
        return avgTicketPrice.hashCode() * 163 + totalPassengersCount.hashCode();
    }


    @Override
    public String toString() {
        return totalPassengersCount+"\t"+avgTicketPrice;
    }
}