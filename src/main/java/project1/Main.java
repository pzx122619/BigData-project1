package project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new java.util.Date());

        int res = 0;

        if (true) {
            res = ToolRunner.run(getLocalConfig(), main, new String[]{
                    "zestaw16/input/datasource1/",
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
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Job job = Job.getInstance(conf, "Film Popularity Stats");
        job.setJarByClass(this.getClass());

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(inputDir, false);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.getPath().getName().startsWith("views")) {
                FileInputFormat.addInputPath(job, file.getPath());
            }
        }

        FileOutputFormat.setOutputPath(job, outputDir);

        job.setMapperClass(ViewsMapper.class);
        job.setCombinerClass(ViewsCombiner.class);
        job.setReducerClass(ViewsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        int result = job.waitForCompletion(true) ? 0 : 1;

        if (result == 0) {
            Path resultFile = new Path(outputDir, "result.csv");
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(resultFile, true)))) {
                bw.write("film_id,platform,total_views,avg_watch_time\n");

                FileStatus[] outputFiles = fs.listStatus(outputDir, path ->
                        path.getName().startsWith("part-r-"));

                for (FileStatus fileStatus : outputFiles) {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            bw.write(line);
                            bw.newLine();
                        }
                    }
                    fs.delete(fileStatus.getPath(), false);
                }
            }
            System.out.println("✅ Wynik zapisany do: " + resultFile.toString());
        }

        return result;
    }
}
