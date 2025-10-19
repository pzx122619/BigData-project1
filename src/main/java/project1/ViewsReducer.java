package project1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ViewsReducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int totalViews = 0;
        double totalTime = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            totalViews += Integer.parseInt(parts[0]);
            totalTime += Double.parseDouble(parts[1]);
        }

        double avgWatchTime = totalTime / totalViews;

        context.write(NullWritable.get(), new Text(key.toString() + "," +
                totalViews + "," + String.format("%.2f", avgWatchTime)));
    }
}
