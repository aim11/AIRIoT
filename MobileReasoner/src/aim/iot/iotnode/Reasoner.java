package aim.iot.iotnode;

import android.util.Log;

import com.hp.hpl.jena.rdf.model.Model;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;

import aim.iot.iotnode.reasoner.IoTReasoner;

/**
 * Created by amaarala on 9/3/14.
 */
public class Reasoner implements Runnable {
    private String rules;
    private String ontology;
    private InputStream data;
    private Model infmodel;
    private GPSObservationModel dm;
    private InputStream isOnt;
    private InputStream isRules;

    public Reasoner(GPSObservationModel dataModel, String ontology, String rules ) {
        this.dm = dataModel;
        this.rules = rules;
        this.ontology = ontology;
    }

    public Reasoner(InputStream data, InputStream isOnt, InputStream isRules) {
        this.isOnt = isOnt;
        this.isRules = isRules;
        this.data = data;
    }

    public Reasoner(InputStream data, String ontology, String rules) {
        this.ontology = ontology;
        this.rules = rules;
        this.data = data;
    }

    @Override
    public void run() {


        IoTReasoner reasoner = new IoTReasoner();

        if(this.dm != null)
            reasoner.setDataModel(dm.getDataModel());
        if(this.data != null)
            reasoner.createDataModel(data);
        if(rules == null )
            throw new NullPointerException("No rules !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        reasoner.initializeGenericReasoner(ontology, rules);

        //InputStream isData = getResources().openRawResource(R.raw.data);

        Reasoning.updateUI("Started Reasoning!"+(new Date()).toString());
 	Log.w("startr","Started Reasoning!"+(new Date()).toString());
        infmodel = reasoner.inferModel();
        MQTTClient.subThredCount();
	Log.w("stopr","Reasoning stopped:" +(new Date()).toString());
        Reasoning.updateUI("Reasoning stopped:" +(new Date()).toString());

        /*
        final StringWriter sw = new StringWriter();
        if(infmodel != null){
            infmodel.write(sw);
            //if(mqttproducer!=null) mqttproducer.produce(sw.toString());
            System.out.println(sw.toString());
        }*/
    }


}
