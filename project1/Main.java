package project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceSummaryValue;
import project1.Value.PassengersTicketPriceValue;

import java.util.Date;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        int res = 0;
        res = ToolRunner.run(main, args);
        System.exit(res);
    }

    public static Configuration getLocalConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        return conf;
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "BusTrip");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //TODO: set mapper and reducer class
        job.setMapperClass(BusTripMapper.class);
        job.setReducerClass(BusTripReducer.class);
        job.setCombinerClass(BusTripCombiner.class);

        //TODO: clean up the data types on both levels: intermediate and final
        job.setMapOutputKeyClass(OperatorDepartureKey.class);
        job.setMapOutputValueClass(PassengersTicketPriceValue.class);

        job.setOutputKeyClass(OperatorDepartureKey.class);
        job.setOutputValueClass(PassengersTicketPriceSummaryValue.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}