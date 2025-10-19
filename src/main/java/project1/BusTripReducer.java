package project1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BusTripReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private final DoubleWritable resultValue = new DoubleWritable();
    Float average;
    Float count;
    int sum;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        average = 0f;
        count = 0f;
        sum = 0;

        Text resultKey = new Text("average station size in " + key + " was: ");

        for (IntWritable val : values) {
            sum += val.get();
            count += 1;
        }
        //TODO: set average variable properly
        double average = (count == 0) ? 0.0 : ((double) sum) / count;

        resultValue.set(average);
        //TODO: write result pair to the context
        context.write(resultKey, resultValue);
    }
}