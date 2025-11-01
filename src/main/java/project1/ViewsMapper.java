package project1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import project1.Key.FilmPlatformKey;
import project1.Value.ViewsWatchTimeValue;

import java.io.IOException;

public class ViewsMapper extends Mapper<LongWritable, Text, FilmPlatformKey, ViewsWatchTimeValue> {

    private final FilmPlatformKey outKey = new FilmPlatformKey();
    private final ViewsWatchTimeValue outValue = new ViewsWatchTimeValue();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("view_id")) {
            return;
        }

        String[] fields = line.split(",", -1);

        String filmId = fields[1].trim();
        String platform = fields[6].trim();
        String durationStr = fields[4].trim();

        double durationSeconds = Double.parseDouble(durationStr);
        double durationMinutes = durationSeconds / 60.0;

        outKey.set(filmId, platform);
        outValue.set(durationMinutes, 1);
        context.write(outKey, outValue);
    }
}