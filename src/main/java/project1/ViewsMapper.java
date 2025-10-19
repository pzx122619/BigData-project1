package project1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ViewsMapper extends Mapper<LongWritable, Text, Text, Text> {
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

        try {
            double durationSeconds = Double.parseDouble(durationStr);
            double durationMinutes = durationSeconds / 60.0;
            context.write(
                    new Text(filmId + "," + platform),// witatable
                    new Text("1," + durationMinutes) // witatable klucz wartośc
            );
        } catch (NumberFormatException e) {
            //
        }
    }
}