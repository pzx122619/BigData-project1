package project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import project1.Key.FilmPlatformKey;
import project1.Value.ViewsWatchTimeSummaryValue;
import project1.Value.ViewsWatchTimeValue;

import java.io.*;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new java.util.Date());

        int res = 0;

        if (false) {
            res = ToolRunner.run(getLocalConfig(), main, new String[]{
                    "zestaw16/input/datasource1",
                    "output/" + timestamp
            });
        } else {
            res = ToolRunner.run(main, args);
        }
        System.exit(res);
    }

    public static Configuration getLocalConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Views");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ViewsMapper.class);
        job.setReducerClass(ViewsReducer.class);
        job.setCombinerClass(ViewsCombiner.class);

        job.setMapOutputKeyClass(FilmPlatformKey.class);
        job.setMapOutputValueClass(ViewsWatchTimeValue.class);

        job.setOutputKeyClass(FilmPlatformKey.class);
        job.setOutputValueClass(ViewsWatchTimeSummaryValue.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
