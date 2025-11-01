package project1;

import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceValue;

import java.io.IOException;

public class BusTripCombiner extends Reducer<OperatorDepartureKey, PassengersTicketPriceValue, OperatorDepartureKey, PassengersTicketPriceValue> {

    @Override
    public void reduce(OperatorDepartureKey key, Iterable<PassengersTicketPriceValue> values, Context context) throws IOException, InterruptedException {
        PassengersTicketPriceValue outValue = new PassengersTicketPriceValue();

        for (PassengersTicketPriceValue v : values) {
            outValue.sum(v);
        }

        context.write(key, new PassengersTicketPriceValue(
                outValue.getPassengersCount().get(),
                outValue.getTicketPrice().get(),
                outValue.getTripsCount().get()
        ));
    }
}