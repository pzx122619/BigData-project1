package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusTripMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text year = new Text();
    private final IntWritable size = new IntWritable();

    public void map(LongWritable offset, Text lineText, Context context) {
        System.out.println(lineText);
//        try {
//            if (offset.get() != 0) {
//                String line = lineText.toString();
//                int i = 0;
//                for (String word : line
//                        .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
//                    if (i == 4) {
//                        year.set(word.substring(word.lastIndexOf('/') + 1,
//                                word.lastIndexOf('/') + 5));
//                    }
//                    if (i == 5) {
//                        size.set(Integer.parseInt(word));
//                    }
//                    i++;
//                }
//                //TODO: write intermediate pair to the context
//                context.write(year, size);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }


}