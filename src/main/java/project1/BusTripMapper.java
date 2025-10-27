package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceValue;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BusTripMapper extends Mapper<LongWritable, Text, OperatorDepartureKey, PassengersTicketPriceValue> {

    private final Text year = new Text();
    private final IntWritable size = new IntWritable();

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String line = lineText.toString();
        if (line.contains("operator_id")) {
            return;
        }
        String[] values = line.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
        LocalDateTime dateTime = LocalDateTime.parse(values[4], formatter);

        Integer hour = dateTime.getHour();
        String operatorId = values[1];
        Double ticketPrice = Double.parseDouble(values[7]);
        Integer passengersCount = Integer.parseInt(values[6]);

        context.write(new OperatorDepartureKey(operatorId, hour), new PassengersTicketPriceValue(passengersCount, ticketPrice));
    }


}