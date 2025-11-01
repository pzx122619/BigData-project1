package project1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.FilmPlatformKey;
import project1.Value.ViewsWatchTimeSummaryValue;
import project1.Value.ViewsWatchTimeValue;

import java.io.IOException;

public class ViewsReducer extends Reducer<FilmPlatformKey, ViewsWatchTimeValue, FilmPlatformKey, ViewsWatchTimeSummaryValue> {

    @Override
    protected void reduce(FilmPlatformKey key, Iterable<ViewsWatchTimeValue> values, Context context)
            throws IOException, InterruptedException {

        double totalWatchTime = 0.0;
        int totalViews = 0;

        for (ViewsWatchTimeValue val : values) {
            totalWatchTime += val.getWatchTime().get();
            totalViews += val.getViewsCount().get();
        }

        double avgWatchTime = (totalViews == 0) ? 0.0 : totalWatchTime / totalViews;

        context.write(key, new ViewsWatchTimeSummaryValue(avgWatchTime, totalViews));
    }
}
