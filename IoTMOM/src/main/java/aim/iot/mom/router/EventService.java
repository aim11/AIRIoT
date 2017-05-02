package aim.iot.mom.router;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 10.1.2014
 * Time: 12:04
 * To change this template use File | Settings | File Templates.
 */


public class EventService {

    protected ProducerTemplate activeMQStatusProducer;
    private String eventQueue;
    private String publicQueue;
    private String packetQueue;

    private static boolean routesInitialized = false;

    public static long reasoninglatency = 0;
    public static int count = 0;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(JMSEventHandler.class);

    @Autowired
    CamelContext camel;


    public EventService() {
    }

    public EventService(String eventQueue, String publicQueue, String packetQueue) {
        this.eventQueue = eventQueue;
        this.publicQueue = publicQueue;
        this.packetQueue = packetQueue;
    }

    @PreDestroy
    public void cleanUp() throws Exception {
        System.out.println("AVG Reasoning latency:" + (double)reasoninglatency/(double)count);
    }

    @PostConstruct
    private void initialize() {

        if (!routesInitialized) {

            // Process messages to ActiveMQ
            activeMQStatusProducer = camel.createProducerTemplate();

            // Process messages from ActiveMQ
            final RouteBuilder builder = new RouteBuilder() {
                @Override
                public void configure() throws Exception {


                    //CorrelationID can be any field such as TYPE, from active mq rest it should be expressed as query param

                    //from("activemq:queue:gps").aggregate(header("JMSCorrelationID"), new StringAggregationStrategyRDFXML()).completionSize(2).process(new RDFXMLEventHandler());
                    from("activemq:queue:rdf").process(new RDFXMLEventHandler());
                    from("activemq:queue:en").process(new ENEventHandler());
                    from("activemq:queue:jsonld").process(new JsonLDEventHandler());

                    //from("activemq:en.events").aggregate(header("JMSCorrelationID"), new StringAggregationStrategy()).completionSize(100).process(new ENEventHandler());
                    //from("activemq:json.events").aggregate(header("JMSCorrelationID"), new StringAggregationStrategyJSONLD()).completionSize(100).process(new JsonLDEventHandler());
                    //from("activemq:rdfxml.queue").aggregate(header("JMSCorrelationID"), new StringAggregationStrategyRDFXML()).completionSize(100).process(new RDFXMLEventHandler());
                    //from("activemq:n3.queue").aggregate(header("JMSCorrelationID"), new StringAggregationStrategy()).completionSize(100).process(new JMSEventHandler());

                    //MULTIPLE QUEUES
                    //for(int i=0; i<=100; i++)
                        //from("activemq:obs.queue_"+i).aggregate(header("JMSCorrelationID"), new StringAggregationStrategy()).completionSize(100).process(new ENEventHandler());
                        //from("activemq:obs.queue_"+i).aggregate(header("JMSCorrelationID"), new StringAggregationStrategyRDFXML()).completionSize(100).process(new RDFXMLEventHandler());


                    //Traffic disord route
                    //from("activemq:events.application/rdf+xml").aggregate(header("JMSCorrelationID"), new StringAggregationStrategyRDFXML()).completionSize(100).process(new RDFXMLEventHandler()).to("activemq:traffic.disorders");

                    //from("activemq:obs.queue_rdfxml").aggregate(header("JMSCorrelationID"), new StringAggregationStrategyRDFXML()).completionSize(100).process(new RDFXMLEventHandler()).to("activemq:trafficdata");


                }
            };

            try {
                camel.addRoutes(builder);
            } catch (Exception e) {

            }

            routesInitialized = true;
        }
    }
}
