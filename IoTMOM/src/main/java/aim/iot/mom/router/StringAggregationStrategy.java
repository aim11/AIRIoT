package aim.iot.mom.router;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 13.1.2014
 * Time: 13:29
 * To change this template use File | Settings | File Templates.
 */
//simply combines Exchange String body values using '+' as a delimiter
class StringAggregationStrategy implements AggregationStrategy {

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            return newExchange;
        }

        String oldBody = oldExchange.getIn().getBody(String.class);
        String newBody = newExchange.getIn().getBody(String.class);
        oldExchange.getIn().setBody(oldBody + " " + newBody);
        return oldExchange;
    }
}
