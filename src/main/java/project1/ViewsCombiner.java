package project1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.FilmPlatformKey;
import project1.Value.ViewsWatchTimeValue;

import java.io.IOException;

public class ViewsCombiner extends Reducer<FilmPlatformKey, ViewsWatchTimeValue, FilmPlatformKey, ViewsWatchTimeValue> {

    @Override
    protected void reduce(FilmPlatformKey key, Iterable<ViewsWatchTimeValue> values, Context context)
            throws IOException, InterruptedException {

        double totalWatchTime = 0.0;
        int totalViews = 0;

        for (ViewsWatchTimeValue val : values) {
            totalWatchTime += val.getWatchTime().get();
            totalViews += val.getViewsCount().get();
        }

        context.write(key, new ViewsWatchTimeValue(totalWatchTime, totalViews));
    }
}
