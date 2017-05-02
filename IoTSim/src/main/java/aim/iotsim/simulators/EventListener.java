package aim.iotsim.simulators;

import aim.iotreasoner.IoTReasoner;
import com.hp.hpl.jena.rdf.model.Model;
import org.apache.activemq.command.ActiveMQBytesMessage;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.UnsupportedEncodingException;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 13.1.2014
 * Time: 11:57
 * To change this template use File | Settings | File Templates.
 */
public class EventListener implements MessageListener {

    private String owlFile = "/home/amaarala/thesis/dev/IoT/IoTSim/ontology.owl";
    private String ruleFile = "file:/home/amaarala/thesis/dev/IoT/IoTSim/rules_jena.txt";


    @Override
    public void onMessage(Message message) {
        //To change body of implemented methods use File | Settings | File Templates.
       System.out.println("Received JMS message:"+message.toString());

        IoTReasoner ioTReasoner = new IoTReasoner(owlFile, "http://localhost/SensorSchema/ontology#", "obs", ruleFile);

        //String[] splitted = message.getBody().toString().split("#&&##");
        ActiveMQBytesMessage bytesmsg = (ActiveMQBytesMessage) message;
        byte[] bs = bytesmsg.getContent().getData();
        String str = "";
        try {
            str = new String(bs, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        ioTReasoner.setDataFormat("RDF/XML");
        String msg = null;
        msg = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:obs=\"http://localhost/SensorSchema/ontology#\">"+str+"</rdf:RDF>";
        ioTReasoner.createDataModel(msg);

        //Store instances
        //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
        String[] types = {};
        Model model = ioTReasoner.inferModel(types, true);

        //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());

        // InfModel m = ioTReasoner.getInferredModel();

        //TODO: route back to topic
        /*if(!model.isEmpty()){

            StringWriter str = new StringWriter();
            //write to JSON-LD
            //TODO: do not write whole model, only inferred
            RDFDataMgr.write(str, model, RDF);
            exchange.getIn().setBody(str.toString());
            org.apache.camel.Message outmsg = (org.apache.camel.Message) exchange.getIn();
            exchange.setOut(outmsg);
        }*/
    }

    public void processStream(Message message) {
        //To change body of implemented methods use File | Settings | File Templates.
        System.out.println("Received aggregated JMS message:"+message.toString());
    }

    public void processPacket(Message message) {
        //To change body of implemented methods use File | Settings | File Templates.
        System.out.println("Received JMS message::"+message.toString());
    }
}
