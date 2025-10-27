package project1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceValue;

import java.io.IOException;

public class BusTripCombiner extends Reducer<OperatorDepartureKey, PassengersTicketPriceValue, Text, SumCount> {


    @Override
    public void reduce(OperatorDepartureKey key, Iterable<PassengersTicketPriceValue> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
    }
}