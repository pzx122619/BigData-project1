package project1;

import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceValue;

import java.io.IOException;

public class BusTripCombiner extends Reducer<OperatorDepartureKey, PassengersTicketPriceValue, OperatorDepartureKey, PassengersTicketPriceValue> {

    private final PassengersTicketPriceValue outValue = new PassengersTicketPriceValue();
    @Override
    public void reduce(OperatorDepartureKey key, Iterable<PassengersTicketPriceValue> values, Context context) throws IOException, InterruptedException {
        for (PassengersTicketPriceValue v : values) {
            System.out.println("Trips: "+outValue.getTripsCount());
            System.out.println("PassengersCount: "+outValue.getPassengersCount());
        }

        context.write(key, outValue);
    }
}