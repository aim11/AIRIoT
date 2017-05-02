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
class StringAggregationStrategyRDFXML implements AggregationStrategy {

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            String newBody = newExchange.getIn().getBody(String.class);
            newBody = newBody.substring(127,newBody.length()-11);
            newExchange.getIn().setBody(newBody);
            return newExchange;
        }

        String oldBody = oldExchange.getIn().getBody(String.class);

        String newBody = newExchange.getIn().getBody(String.class);
        newBody = newBody.substring(127,newBody.length()-11);
        oldExchange.getIn().setBody(oldBody + newBody);
        return oldExchange;
    }
}
