package project1;

import org.apache.hadoop.mapreduce.Reducer;
import project1.Key.OperatorDepartureKey;
import project1.Value.PassengersTicketPriceSummaryValue;
import project1.Value.PassengersTicketPriceValue;

import java.io.IOException;

public class BusTripReducer extends Reducer<OperatorDepartureKey, PassengersTicketPriceValue, OperatorDepartureKey, PassengersTicketPriceSummaryValue> {

    private final PassengersTicketPriceSummaryValue outValue = new PassengersTicketPriceSummaryValue();

    @Override
    public void reduce(OperatorDepartureKey key, Iterable<PassengersTicketPriceValue> values,
                       Context context) throws IOException, InterruptedException {

        PassengersTicketPriceValue summary = new PassengersTicketPriceValue();
        for (PassengersTicketPriceValue v : values) {
            summary.sum(v);
        }


        outValue.set(
                summary.getTicketPrice().get()/summary.getTripsCount().get(),
                summary.getPassengersCount().get()
        );

        context.write(key, outValue);
    }
}