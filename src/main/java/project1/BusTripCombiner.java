package project1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BusTripCombiner extends Reducer<Text, SumCount, Text, SumCount> {

    private final SumCount sum = new SumCount(0.0d, 0);

    @Override
    public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

        sum.set(new DoubleWritable(0.0d), new IntWritable(0));

        for (SumCount val : values) {
            sum.addSumCount(val);
        }
        context.write(key, sum);
    }
}