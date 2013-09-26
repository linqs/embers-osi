package edu.umd.cs.linqs.embers

import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.application.inference.LazyMPEInference;
import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.evaluation.result.FullInferenceResult;
import edu.umd.cs.psl.model.Model
import edu.umd.cs.psl.model.argument.StringAttribute;
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.UniqueID;
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.kernel.ConstraintKernel;
import edu.umd.cs.psl.model.kernel.Kernel
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.model.predicate.PredicateFactory
import edu.umd.cs.psl.optimizer.conic.partition.ObjectiveCoefficientCompletePartitioner;
import edu.umd.cs.psl.parser.PSLModelLoader
import edu.umd.cs.psl.util.database.Queries;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.text.DateFormat
import java.text.SimpleDateFormat

class PSLJointRewriter implements JSONProcessor {

	private DataStore data;
	private final ConfigBundle cb;

	private final Logger log = LoggerFactory.getLogger(this.class)
	private final ConfigManager cm = ConfigManager.getManager()

	private final String CONFIG_PREFIX = "pslrewriter";

	/* Key for model filename */
	private final String MODEL_FILENAME = CONFIG_PREFIX + ".model";

	private final String defaultPath = System.getProperty("java.io.tmpdir");

	private final String BLANK = "-";


	private Partition read;
	private Partition write;

	private Model m;

	private final int readPartNum = 0;
	private final int writePartNum = 1;

	private Predicate embersViolPred;
	private Predicate embersTypePred;
	private Predicate embersPopPred;

	private Predicate maxTypePred;
	private Predicate maxPopPred;
	private Predicate maxViolPred;

	private Predicate secondTypePred;
	private Predicate secondPopPred;

	private Predicate countryPred;

	private Predicate popPred;
	private Predicate typePred;
	private Predicate violPred;

	public PSLJointRewriter() {
		cb = cm.getBundle(CONFIG_PREFIX);

		String dbPath = cb.getString("dbpath", defaultPath);
		String dbName = cb.getString("dbname", "psl");
		String fullDBPath = dbPath + dbName;

		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);
		read = new Partition(readPartNum);
		write = new Partition(writePartNum);

		String savedModel = cb.getString("model", "");

		m = PSLModelLoader.loadModel(savedModel, data);


		// load predicates

		PredicateFactory pf = PredicateFactory.getFactory();

		embersViolPred = pf.getPredicate("embersViolent");
		embersTypePred = pf.getPredicate("embersType");
		embersPopPred = pf.getPredicate("embersPopulation");

		maxViolPred = pf.getPredicate("maxViolent");
		maxTypePred = pf.getPredicate("maxType");
		maxPopPred = pf.getPredicate("maxPopulation");

		secondTypePred = pf.getPredicate("secondType");
		secondPopPred = pf.getPredicate("secondPopulation");

		countryPred = pf.getPredicate("country");
		popPred = pf.getPredicate("population");
		typePred = pf.getPredicate("type");
		violPred = pf.getPredicate("violent");

	}

	@Override
	public String process(String json) {
		JSONObject object = new JSONObject(json);

		// point new object to old embersId
		if (!object.has("derivedFrom"))
			object.put("derivedFrom", new JSONObject());
		JSONObject derivedFrom = object.getJSONObject("derivedFrom");
		if (!derivedFrom.has("derivedIds"))
			derivedFrom.put("derivedIds", new JSONArray());
		derivedFrom.getJSONArray("derivedIds").put(object.getString("embersId"));

		String oldComments = "";
		if (object.has("comments"))
			oldComments = object.getString("comments") + ", ";
		object.put("comments", oldComments + "post-processed using PSL joint predictor v. 0.1");

		// generate new embersId
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));

		String newID = getHash(dateFormat.format(date) + object.toString());

		object.put("embersId", newID);

		if (object.has("classification")) {

			data.deletePartition(read);
			data.deletePartition(write);

			System.out.println("Processing " + json);

			Database db = data.getDatabase(read);

			jsonToPSL(object, db);

			// do inference
			db = data.getDatabase(write, read);
			LazyMPEInference mpe = new LazyMPEInference(m, db, cb);
			FullInferenceResult result = mpe.mpeInference();

			// Read out inferred predictions
			Set<GroundAtom> results;
			GroundAtom best;

			results = Queries.getAllAtoms(db, typePred);
			best = null;
			for (GroundAtom atom : results)
				if (best == null || atom.getValue() > best.getValue())
					best = atom;
			String newType = best.getArguments()[1].toString();

			results = Queries.getAllAtoms(db, popPred);
			best = null;
			for (GroundAtom atom : results)
				if (best == null || atom.getValue() > best.getValue())
					best = atom;
			String newPop = best.getArguments()[1].toString();

			results = Queries.getAllAtoms(db, violPred);
			best = null;
			for (GroundAtom atom : results)
				if (best == null || atom.getValue() > best.getValue())
					best = atom;
			String newViol = best.getArguments()[1].toString();

			db.close();

			object.put("population", newPop);

			object.put("eventType", newType + newViol);
		}

		return object.toString();
	}

	private void jsonToPSL(JSONObject object, Database db) {
		
		String id = object.getString("embersId");
		String country = object.getJSONArray("location").getString(0);
		insertAtom(1.0, db, countryPred, id, country);

		// load predications
		Map<String, Double> embersType = new HashMap<String, Double>();
		JSONObject eventType = object.getJSONObject("classification").getJSONObject("eventType");
		for (String key : eventType.keys()) {
			embersType.put(key, eventType.getDouble(key));
			insertAtom(eventType.getDouble(key), db, embersTypePred, id, key);
		}

		// load populations
		Map<String, Double> embersPop = new HashMap<String, Double>();
		JSONObject population = object.getJSONObject("classification").getJSONObject("population");
		for (String key : population.keys()) {
			embersPop.put(key, population.getDouble(key));
			insertAtom(population.getDouble(key), db, embersPopPred, id, key);
		}

		// load violence
		Map<String, Double> embersViol= new HashMap<String, Double>();
		JSONObject violence = object.getJSONObject("classification").getJSONObject("violence");
		for (String key : violence.keys()) {
			embersViol.put(key, violence.getDouble(key));
			insertAtom(violence.getDouble(key), db, embersViolPred, id, key);
		}

		// insert max and second best values
		Map.Entry<String, Double> max;

		max = removeMax(embersType); // find highest scoring entry
		insertAtom(1.0, db, maxTypePred, id, max.getKey());
		insertAtom(1.0, db, secondTypePred, id, max.getKey());
		max = removeMax(embersType); // find second-highest scoring entry
		insertAtom(1.0, db, secondTypePred, id, max.getKey());

		max = removeMax(embersPop); // find highest scoring entry
		insertAtom(1.0, db, maxPopPred, id, max.getKey());
		insertAtom(1.0, db, secondPopPred, id, max.getKey());
		max = removeMax(embersPop); // find second-highest scoring entry
		insertAtom(1.0, db, secondPopPred, id, max.getKey());

		max = removeMax(embersViol); // find highest scoring entry
		insertAtom(1.0, db, maxViolPred, id, max.getKey());
		db.close();
	}
	
	private Map.Entry<String, Double> removeMax(Map<String, Double> map) {
		Map.Entry<String, Double> best = null;

		for (Map.Entry<String, Double> e : map.entrySet()) {
			if (best == null || e.getValue() > best.getValue())
				best = e;
		}

		map.remove(best.getKey());
		return best;
	}

	private void insertAtom(Double value, Database db, Predicate pred, String ... args) {
		GroundTerm [] convertedArgs = Queries.convertArguments(db, pred, args);

		RandomVariableAtom atom = db.getAtom(pred, convertedArgs);
		atom.setValue(value);
		atom.commitToDB();
	}

	private String getHash(String text) {
		MessageDigest md = MessageDigest.getInstance("SHA-256");

		md.update(text.getBytes("UTF-8"));

		byte [] hash = md.digest();

		return DatatypeConverter.printHexBinary(hash);
	}
}
