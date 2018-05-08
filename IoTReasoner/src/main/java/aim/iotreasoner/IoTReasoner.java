package aim.iotreasoner;

import aim.iotreasoner.database.SesameAdapter;
import aim.iotreasoner.querying.QueryBuilder;
import aim.iotreasoner.querying.QueryEngine;
import com.github.jsonldjava.jena.JenaJSONLD;

import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.rdf.model.Model;

import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.rulesys.ClauseEntry;
import com.hp.hpl.jena.reasoner.rulesys.GenericRuleReasoner;
import com.hp.hpl.jena.reasoner.rulesys.Rule;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.PrintUtil;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.vocabulary.RDF;


import org.apache.jena.riot.RDFDataMgr;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 26.11.2013
 * Time: 16:47
 * To change this template use File | Settings | File Templates.
 */
public class IoTReasoner {

    private String ontology = "./traffic.owl";
    private String graphURI = "http://localhost/Schema/ontology#";
    private String prefix = "obs";
    private String rulesFile = "./traffic.rules";


    private InfModel rulesmodel = null;

    private String dataFormat = "N3";

    public Model dataModel = null;
    private OntModel ontologyModel = null;
    static public SesameAdapter sesameAdapter = null;

    //private static IoTReasoner iotreasoner = null;
    private Model schema = null;

    private Reasoner ruleReasoner = null;
    private RepositoryConnection con =null;

    //private OntModel ontmodel = null;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(IoTReasoner.class);

    private Model collectedInfModel;
    private List rules = new ArrayList<Rule>();
    private String predictClasses;

    public Model getDeductionsModel() {
        return deductionsModel;
    }

    private Model deductionsModel;

    public IoTReasoner() {
        this.getProperties();
        this.setRulesFile(rulesFile);
        this.setPrefix(prefix);
        this.setGraphURI(graphURI);
        this.setOntology(ontology);
        /*if(iotreasoner == null){
            iotreasoner = this;
        }*/
        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( this.dataModel, "observations.rdf" );
        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();

    }

    public IoTReasoner(String ontology, String graphURI, String prefix, String rulesFile) {
        this.getProperties();
        this.setRulesFile(rulesFile);
        this.setPrefix(prefix);
        this.setGraphURI(graphURI);
        this.setOntology(ontology);

        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( this.dataModel, "observations.rdf" );
        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();

    }

    public IoTReasoner(String rulesFile) {
        this.getProperties();
        this.setRulesFile(rulesFile);

        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( this.dataModel, "observations.rdf" );
        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();

    }

    public void initJsonLD(){
        JenaJSONLD.init();
    }


    public IoTReasoner(List<Rule> rl) {
        this.getProperties();

        this.rules = rl;

        schema = ModelFactory.createDefaultModel();

        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();
    }

    public void initializeGenericReasoner(){


        //Reasoner iotreasoner = ReasonerRegistry.getOWLReasoner();

        ruleReasoner = new GenericRuleReasoner(getRules());

        //LPBackwardRuleReasoner lpbrr = new LPBackwardRuleReasoner(getRules());

        ruleReasoner.bindSchema(schema);


    }


    public static OntModel getOntologyModel(ResultSet rs, String prefix, String graphURI){

        OntModel tempModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        tempModel.setNsPrefix(prefix, graphURI);
        //System.out.println(" - " + rs.toString());


        //set data to iotreasoner datamodel from sparql results and ini

        //tempModel.write(System.out, dataFormat);

        while(rs.hasNext()) {
            QuerySolution sol = rs.nextSolution();
            Resource resource = sol.getResource("s");
            RDFNode subject = sol.get("s");
            RDFNode pred = sol.get("p");
            RDFNode obj = sol.get("o");

            //System.out.println(" - " + sol.toString());

            //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );
            Property prop = subject.getModel().getProperty(pred.toString());
            //Property prop = tempModel.createProperty(pred.toString());
            tempModel.add(resource, prop, obj);

        }

        /*StmtIterator stmit = tempModel.listStatements();
        while(stmit.hasNext())         {
            System.out.println(" STMT - " + stmit.nextStatement().toString());
        }*/
        //final StringWriter wr = new StringWriter();

        return tempModel;

    }

    public  OntModel getOntologyModel(ResultSet rs){

        OntModel tempModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        tempModel.setNsPrefix(prefix, graphURI);
        //System.out.println(" - " + rs.toString());


        //set data to iotreasoner datamodel from sparql results and ini

        //tempModel.write(System.out, dataFormat);

        while(rs.hasNext()) {
            QuerySolution sol = rs.nextSolution();
            //System.out.println(" - " + sol.toString());
            Resource resource = sol.getResource("s");
            RDFNode subject = sol.get("s");
            RDFNode pred = sol.get("p");
            RDFNode obj = sol.get("o");

            //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );
            Property prop = subject.getModel().getProperty(pred.toString());
            //Property prop = tempModel.createProperty(pred.toString());
            tempModel.add(resource, prop, obj);

        }

        /*StmtIterator stmit = tempModel.listStatements();
        while(stmit.hasNext())         {
            System.out.println(" STMT - " + stmit.nextStatement().toString());
        }*/
        //final StringWriter wr = new StringWriter();

        return tempModel;

    }

    public OntModel getInferredModel() {
        return ontologyModel;
    }



    public SesameAdapter getSesameAdapter() {
        //TODO: some strange things happening here, format is not changed in all cases and when changed other than RDF/XML storing to repository fails
        if (sesameAdapter == null){
            RDFFormat sformat =  RDFFormat.RDFXML;
            /*if(dataFormat == "N3")
               sformat = org.openrdf.rio.RDFFormat.N3;
            if(dataFormat == "N-TRIPLE")
                sformat = RDFFormat.NTRIPLES;
            if(dataFormat == "TURTLE")
                sformat = org.openrdf.rio.RDFFormat.TURTLE;*/

            sesameAdapter = new SesameAdapter(graphURI, sformat);
        }
        return sesameAdapter;
    }



     public void setGraphURI(String graphURI) {
        this.graphURI = graphURI;
    }

    public void setRulesmodel(InfModel rulesmodel) {
        this.rulesmodel = rulesmodel;
    }


    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public  Reasoner getRuleReasoner() {
        return ruleReasoner;
    }

    public  void setRuleReasoner(Reasoner ruleReasoner) {
        this.ruleReasoner = ruleReasoner;
        ruleReasoner.bindSchema(schema);
    }
    public String getRulesFile() {
        return rulesFile;
    }

    public void setRulesFile(String rulesFile) {
        this.rulesFile = rulesFile;
    }



    public void updateSesameRepository(Model model)  {

        // creates a new, empty in-memory model
       /* RemoteRepositoryManager manager = new RemoteRepositoryManager(sesameServer);
        try {
            manager.initialize();
        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }     */
        // creates a new, empty in-memory model

        //Resource ENTITY_TYPE = rulesmodel.getResource("http://localhost/Schema/ontology#Jam");
        /*StmtIterator it = rulesmodel.getDeductionsModel().listStatements(null, RDF.type, ENTITY_TYPE);

        while (it.hasNext()) {
            System.out.println(" - " + PrintUtil.print(it.nextStatement().toString()));
            //addStatement(getSesameConnection(), it.nextStatement());
        }*/

         //write statements as RDF/XML
        final StringWriter wr = new StringWriter();
        //inferredModel.write(System.out, dataFormat);

        model.write(wr, null, dataFormat);

        //Add statements to RDF DB

        getSesameAdapter().addStatements(getSesameAdapter().createConnection(), wr);

        //rulesmodel.write(System.out, dataFormat);

        //showModelSize(this.dataModel);


    }

    public Model inferQTModel(String[] classes){

        //TODO: examine if there is way to implement kinda backward chained inference
        // where we do not need to query body rules beforehand
        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : getRules()){

            for(int i=0; i<classes.length; i++){
                if(rule.getName().equals(classes[i])){
                    ClauseEntry[] body = rule.getBody();
                    String type1 = body[0].toString().split(" ")[2];
                    String type2 = body[1].toString().split(" ")[2];
                    types.add(type1);
                    types.add(type2);

                }
            }
        }



        String query = QueryBuilder.selectAllByType(graphURI, types.toArray(new String[0]));
        ResultSet rs = QueryEngine.query(query);



        this.dataModel = getOntologyModel(rs, prefix, graphURI);

        Model model = inferModel(classes, true);

        return model;

    }

    public void inferQTSpatial(String[] classes){

        //TODO: examine if there is way to implement kinda backward chained inference
        // where we do not need to query body rules beforehand

        String query = QueryBuilder.selectAllByType(graphURI, classes);
        ResultSet rs = QueryEngine.query(query);

        this.dataModel = getOntologyModel(rs, prefix, graphURI);

        inferModel(classes, true);

    }

    //TODO: not working this way, rules must be parsed other way or read from files separately,but it is expensive
    @Deprecated
    public InfModel  inferModelSelective(String[] classes, final boolean store) {


        //Inference

        OntModel newKnowledgeModel = null;
        newKnowledgeModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        newKnowledgeModel.setNsPrefix(prefix, graphURI);

        for(String classType : classes) {

            Set<String> types = new HashSet<String>();
            //Parse rdf:types from rule body
            List<Rule> headRuleList = new ArrayList<Rule>();

            for (Rule rule : this.getRules()) {
                for (int i = 0; i < classes.length; i++) {
                    if (rule.getName().equals(classType)) {
                        headRuleList.add(rule);
                    }
                }
            }

            if(!headRuleList.isEmpty()){
            InfModel infModel = inferFromRule(headRuleList);

            //Property p = dataModel.getProperty(graphURI, "rdf:type");

            //This takes much longer with backward rules
            //TODO: propably there is a bug, does not go like with non-selective, different instances were saved
            OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel);
            OntClass oc = om.getOntClass(graphURI + classType);

            if (oc != null) {
                ExtendedIterator<? extends OntResource> instances = oc.listInstances();
                ExtendedIterator<? extends OntProperty> propit = oc.listDeclaredProperties();
                ArrayList<String> proplist = new ArrayList<String>();

                while (propit.hasNext()) {


                    proplist.add(propit.next().toString());
                }
                while (instances.hasNext()) {

                    OntResource instance = instances.next();

                    //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );

                    //LOGGER.info(" ---------Inferred type--------- " + classType);

                    newKnowledgeModel.add(instance, RDF.type, om.getResource(graphURI + classType));

                    for (String prop : proplist) {
                        //addStatement(getSesameConnection(), it.nextStatement());
                        try {
                            if (instance.getProperty(om.getOntProperty(prop)) != null) {
                                //System.out.println(prop + ": " + instance.getProperty(om.getOntProperty(prop)).getString());

                                newKnowledgeModel.add(instance, om.getOntProperty(prop), instance.getProperty(om.getOntProperty(prop)).getString());
                            }
                        } catch (NoSuchElementException e) {

                        }
                    }

                    //TODO: try adding one statement at time

                }

            }  }


        }
        if (store == true)
            updateSesameRepository(newKnowledgeModel);

        return newKnowledgeModel;


    }

    public InfModel inferModel2(String[] classes, boolean store){

        //Inference
        InfModel infModel = inferFromRules();

   /*     List<String> interestingEvents = Arrays.asList(classes);

        OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel);

        //LOGGER.info(" --------- inferred --------- "+((new Date()).getTime()-time));

        OntModel newKnowledgeModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        newKnowledgeModel.setNsPrefix(prefix, graphURI);
    */
        /*
        //TESTING all rules
        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : getRules()){

            types.add(rule.getName());


        }*/

        this.updateSesameRepository(infModel.getDeductionsModel());

        return infModel;
    }

    public Model inferModel(String[] classes, boolean store){

       // String[] classes = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop", "HighAcceleration", "HighDeAcceleration", "VeryLongStop"};
        //String[] classes = {"RightTurn", "LeftTurn", "UTurn", "Jam",  "LongStop"};


        //Inference
        InfModel infModel = inferFromRules();

        if(classes == null)
            classes = this.predictClasses.split(",");
        List<String> interestingEvents = Arrays.asList(classes);

        OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel);

        //LOGGER.info(" --------- inferred --------- "+((new Date()).getTime()-time));

        //OntModel newKnowledgeModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        //newKnowledgeModel.setNsPrefix(prefix, graphURI);

        Model inferencedInstancesModel = ModelFactory.createDefaultModel();
        inferencedInstancesModel.setNsPrefixes(infModel.getNsPrefixMap());
        /*
        //TESTING all rules
        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : getRules()){

            types.add(rule.getName());


        }*/



        for(String classType : interestingEvents) {
            //Property p = dataModel.getProperty(graphURI, "rdf:type");

            //This takes much longer with backward rules
            //TODO: optimize for backward rules
            OntClass oc = om.getOntClass(graphURI + classType);

            if(oc != null){
                ExtendedIterator<? extends OntResource> instances = oc.listInstances();
                ExtendedIterator<? extends OntProperty> propit = oc.listDeclaredProperties();
                ArrayList<String> proplist = new ArrayList<String>();

                while (propit.hasNext()) {


                    proplist.add(propit.next().toString());
                }
                while (instances.hasNext()) {

                    OntResource instance = instances.next();

                    //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );

                    //LOGGER.info(" ---------Inferred type--------- " + classType);

                    inferencedInstancesModel.add(instance, RDF.type, om.getResource(graphURI + classType));

                    for (String prop : proplist) {
                        //addStatement(getSesameConnection(), it.nextStatement());
                        try{
                            if(instance.getProperty(om.getOntProperty(prop))!=null){
                                //System.out.println(prop + ": " + instance.getProperty(ontmodel.getOntProperty(prop)).getString());

                                inferencedInstancesModel.add(instance, om.getOntProperty(prop), instance.getProperty(om.getOntProperty(prop)).getString());
                            }
                        }catch(NoSuchElementException e){

                        }
                    }

                    //TODO: try adding one statement at time
                    //updateSesameRepository(ioTReasoner.getInferredModel());
                }

            }

        }

        if(store == true && !inferencedInstancesModel.isEmpty())
            this.updateSesameRepository(inferencedInstancesModel);

        //tempModel.write(System.out);
        //this.ontologyModel = inferencedInstancesModel;

        return inferencedInstancesModel;

    }


    public InfModel inferFromRules() {

        long time = (new Date()).getTime();
        //System.out.println(" ---------Infer --------- "+ time);
        this.rulesmodel = ModelFactory.createInfModel(ruleReasoner, dataModel);

        //System.out.println(" ---------inferred 1--------- "+((new Date()).getTime()-time));
        this.rulesmodel.setNsPrefix(prefix, graphURI);

        //Deductions model not created from backward rules
        //to get only deducted triples strictly from the model,
        rulesmodel.getDeductionsModel().setNsPrefix("obs", "http://localhost/Schema/ontology#");
        this.deductionsModel = rulesmodel.getDeductionsModel();
        //updateSesameRepository(rulesmodel.getDeductionsModel());

        return rulesmodel;
    }

    public InfModel inferFromRule(List<Rule> rules) {

        long time = (new Date()).getTime();
        //System.out.println(" ---------Infer --------- "+ time);

        GenericRuleReasoner reasoner = new GenericRuleReasoner(rules);
        reasoner.bindSchema(schema);

        //LPBackwardRuleReasoner lpbrr = new LPBackwardRuleReasoner(getRules());

        InfModel infModel = ModelFactory.createInfModel(reasoner, dataModel);

        //System.out.println(" ---------inferred 1--------- "+((new Date()).getTime()-time));
        infModel.setNsPrefix(prefix, graphURI);

        //Deductions model not created from backward rules
        //to get only deducted triples strictly from the model,
        infModel.getDeductionsModel().setNsPrefix("obs", "http://localhost/Schema/ontology#");

        //updateSesameRepository(rulesmodel.getDeductionsModel());

        //OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel.getDeductionsModel());

        //LOGGER.info(" --------- inferred --------- "+((new Date()).getTime()-time));


        return infModel;
    }



    protected void printStatements(Model m, Resource s, Property p, Resource o) {
        for (StmtIterator i = m.listStatements(s,p,o); i.hasNext(); ) {
            Statement stmt = i.nextStatement();
            System.out.println(" - " + PrintUtil.print(stmt));
        }
    }

    protected void showModelSize( Model m ) {
        System.out.println( String.format( "The model contains %d triples", m.size() ) );
    }

    /*public static IoTReasoner getReasoner() {
        return this;
    }*/

    public List<Rule> getRules(){
        //GPSObservation(?o), hasVelocity(?o, ?v), lessThan(?v, "5"^^int) -> Stop1(?o)
        //String stop1 =   "[stop1: (?d rdf:type obs:Observation)(?d obs:hasVelocity ?v) lessThan(?v,'5'^^xsd:integer) -> (?d rdf:type obs:Stop1)]";
        //GPSObservation(?o), Stop1(?j1), hasID(?j1, ?ID1), hasID(?o, ?ID2), hasSender(?j1, ?s), hasSender(?o, ?s), hasVelocity(?o, ?v), add(?ID2, ?ID1, "1"^^int), lessThan(?v, 3) -> Stop2(?o)
        List<Rule> rules = new ArrayList<Rule>();
        if(this.rules.isEmpty() && this.rulesFile != null) {
            rules = Rule.rulesFromURL(this.rulesFile);
            this.rules = rules;
        }
        /*Map<String, List<Rule>> rulez = new HashMap<String, List<Rule>>();

        for (Rule rule : rules) {
            if(rulez.containsKey(rule.getName()))
                rulez.get(rule.getName()).add(rule);
            else {
                List<Rule> rl = new ArrayList<Rule>();
                rl.add(rule);
                rulez.put(rule.getName(), rl);
            }
        }*/

        return this.rules;
    }



    public Model getDataModel() {
        return dataModel;
    }

    public Model getSchema() {
        return schema;
    }

    public void initDataModel(Model dataModel) {
        this.dataModel = dataModel;
        //inferFromRules();
        //initOntmodel();
    }
    public void setOntology(String ontology) {
        this.ontology = ontology;
    }


    public void createDataModel(String rdfData) {

        Model model = ModelFactory.createDefaultModel();
        StringReader reader = new StringReader(rdfData);
        try{
             model.read(reader, null, dataFormat);
        }catch(org.apache.jena.riot.RiotException e) {
            e.printStackTrace();
        }
        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }

    public void createDataModelJSONLD(String rdfData) {

        Model model = ModelFactory.createDefaultModel();
        //StringReader reader = new StringReader(rdfData);
        try{
            InputStream inStream = new ByteArrayInputStream(rdfData.getBytes("UTF-8"));
            RDFDataMgr.read(model, inStream, graphURI, JenaJSONLD.JSONLD);
        }catch(org.apache.jena.riot.RiotException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }


    public  void createDataModel(InputStream rdfData) {

        Model model = ModelFactory.createDefaultModel();
        model.read(rdfData, null, dataFormat);

        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }


    public void createDataModel(String[] splitted) {
        Model model = ModelFactory.createDefaultModel();
        for(int i = 0; i<splitted.length; i++) {
            StringReader reader = new StringReader(splitted[i]);
            model.read(reader, null, dataFormat);
        }
        this.dataModel = model;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public void setDataModel(Model dataModel) {
        this.dataModel = dataModel;
    }

    public void getProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");
            prop.load(input);
            ontology = prop.getProperty("owlfile");
            rulesFile = prop.getProperty("rulesfile");
            graphURI = prop.getProperty("ontologyuri");
            predictClasses = prop.getProperty("predict_classnames");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
