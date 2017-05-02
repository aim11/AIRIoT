package aim.iot.mom.router;

import aim.iotreasoner.IoTReasoner;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 10.1.2014
 * Time: 12:03
 * To change this template use File | Settings | File Templates.
 */
public class ENEventHandler implements Processor{

        private static final Logger LOGGER = LoggerFactory
        .getLogger(ENEventHandler.class);

        private String owlFile = "./traffic.owl";
        private String ruleFile = "./traffic.rules";

    private String ontologyURI = "http://localhost/SensorSchema/ontology#";
    static long tt = 0;


    public void process(Exchange exchange) throws Exception {
            try {

                //long reasoningLatency = 0;

                Message message = (Message) exchange.getIn();

                //System.out.println("Received JMS message:" + message.getExchange().getIn().toString());

                Date startd = new Date();
                IoTReasoner ioTReasoner = new IoTReasoner(owlFile, "http://localhost/SensorSchema/ontology#", "obs", ruleFile);

                //long d = (new Date()).getTime();

                Model datamodel = pooledEnToDataModel(message.getBody().toString());

                //tt = tt + ((new Date()).getTime()) - d;
                //LOGGER.info("tt: "+tt);

                ioTReasoner.setDataFormat("N3");
                ioTReasoner.setDataModel(datamodel);


                //Store instances
                //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
                String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop"};
                Model model = ioTReasoner.inferModel(types, true);

                exchange.setOut(exchange.getIn());

            } catch (Throwable e) {
                e.printStackTrace();
                LOGGER.error("failed parsing JMS event {}", e.getMessage());
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }

        }

    public Model pooledEnToDataModel(String enData) {

        Model dataModel = ModelFactory.createDefaultModel();

        String[] enLines = enData.split("]");

        /*if(!enData.substring(0).equals("[")) {
            try {
                throw new Exception("Wrong input data type(propably not EN)");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }*/
        //Statement typeStatement = factory.createStatement(obsURI, RDF.TYPE, obsType);
        //myGraph.add(typeStatement);
        System.out.println("AGGLEN: "+enLines.length);
        dataModel.setNsPrefix("obs", ontologyURI);

        for(int i = 0; i < enLines.length; i++){

            String[] obs = enLines[i].split(" ");

            Resource obsInstance = dataModel.createResource();
            dataModel.add(obsInstance, RDF.type, dataModel.createResource(this.getTemplate()[0]));

            /*for(int j = 0; j < obs.length; j++){
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[j+1]), dataModel.createLiteral(obs[j]));
            }*/

            if(obs.length==13){
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[1]), dataModel.createTypedLiteral(Integer.valueOf(obs[2])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[2]), dataModel.createTypedLiteral(Integer.valueOf(obs[3])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[3]), dataModel.createTypedLiteral(Double.valueOf(obs[4])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[4]), dataModel.createTypedLiteral(Double.valueOf(obs[5])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[5]), dataModel.createTypedLiteral(Double.valueOf(obs[6])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[6]), dataModel.createTypedLiteral(Integer.valueOf(obs[7])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[7]), dataModel.createTypedLiteral(Integer.valueOf(obs[8])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[8]), dataModel.createTypedLiteral(Double.valueOf(obs[9])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[9]), dataModel.createTypedLiteral(Double.valueOf(obs[10])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[10]), dataModel.createTypedLiteral(Long.valueOf(obs[11])));
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[11]), dataModel.createLiteral(obs[12]));
            }
        }
        return dataModel;
    }

    private String[] getTemplate(){
     /* String obsTemplate = "["+ontologyURI+"Observation "+ ontologyURI + "hasID "+ontologyURI + "hasArea "+ontologyURI +
              "hasLatitude "+ontologyURI + "hasLongitude "+ontologyURI + "hasVelocity "+ontologyURI + "hasDirection "+ontologyURI +
              "hasSender "+ontologyURI + "hasDistance "+ontologyURI + "hasAcceleration "+ontologyURI + "hasDate "+ontologyURI + "hasDateTime]";
*/
        String[] obsTemplate = {ontologyURI+"Observation", ontologyURI + "hasID", ontologyURI + "hasArea", ontologyURI +
                "hasLatitude", ontologyURI + "hasLongitude", ontologyURI + "hasVelocity", ontologyURI + "hasDirection", ontologyURI +
                "hasSender", ontologyURI + "hasDistance", ontologyURI + "hasAcceleration", ontologyURI + "hasDate", ontologyURI + "hasDateTime"};


        return obsTemplate;
    }
}
