package edu.umd.cs.linqs.embers

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.MessageDigest
import java.util.zip.GZIPInputStream;

import javax.xml.bind.DatatypeConverter

import org.json.JSONArray;
import org.json.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.application.learning.weight.WeightLearningApplication;
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.MaxLikelihoodMPE;
import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.groovy.PredicateConstraint;
import edu.umd.cs.psl.model.Model
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.StringAttribute;
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.model.predicate.PredicateFactory
import edu.umd.cs.psl.parser.PSLModelLoader;
import edu.umd.cs.psl.util.database.Queries

class PSLRewriterTrainer {

	private DataStore data;
	private final ConfigBundle cb;

	private final Logger log = LoggerFactory.getLogger(this.class)
	private final ConfigManager cm = ConfigManager.getManager()

	private final String CONFIG_PREFIX = "pslrewriter";

	/* Key for model filename */
	private final String MODEL_FILENAME = CONFIG_PREFIX + ".model";

	private final String defaultPath = System.getProperty("java.io.tmpdir");

	private final String BLANK = "-";

	private final double SUPPRESS_THRESHOLD;

	private Partition trainRead;
	private Partition trainWrite;
	private Partition trainLabel;

	private PSLModel m;

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

	private Predicate suppressPred;

	def types = ['011', '012', '013', '014', '015', '016']
	def violents = ['1', '2']
	def pops = ['General Population', 'Business', 'Ethnic', 'Legal', 'Education', 'Religious', 'Medical', 'Media', 'Labor', 'Refugees/Displaced', 'Agricultural']

	def popLookup = [
		'01': "General Population",
		'02': "Business",
		'03': "Ethnic",
		'04': "Legal",
		'05': "Education",
		'06': "Religious",
		'07': "Medical",
		'08': "Media",
		'09': "Labor",
		'10': "Refugees/Displaced",
		'11': "Agricultural"
	];

	/**
	 * Creates and initializes new training object
	 * @param trainFile filename of json-formatted warnings with gsr matched entries
	 */
	public PSLRewriterTrainer(String trainFile) {
		cb = cm.getBundle(CONFIG_PREFIX);

		String dbPath = cb.getString("dbpath", defaultPath);
		String dbName = cb.getString("dbname", "psl");
		String fullDBPath = dbPath + dbName;

		SUPPRESS_THRESHOLD = cb.getDouble("suppressthreshold", 0.0);

		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);

		m = new PSLModel(this, data);

		/* 
		 * Define predicates
		 */
		m.add predicate: "city", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "state", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "country", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "population", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "type", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "suppress", types: [ArgumentType.UniqueID]
		m.add predicate: "violent", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "embersPopulation", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "embersType", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "embersViolent", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "maxPopulation", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "maxType", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "maxViolent", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "secondPopulation", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "secondType", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "suppress", types: [ArgumentType.UniqueID]


		// save predicates as java objects (to avoid using groovy tricks)

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

		suppressPred = pf.getPredicate("suppress");


		/**
		 * Add rules with default weights
		 */

		m.add PredicateConstraint.Functional , on : population
		m.add PredicateConstraint.Functional , on : type
		m.add PredicateConstraint.Functional , on : violent
		def initialWeight = 0.0

		m.add rule: (embersType(E, T)) >> type(E, T), weight: initialWeight, squared: true
		m.add rule: (maxType(E, T)) >> type(E, T), weight: initialWeight, squared: true
		m.add rule: (secondType(E, T)) >> type(E, T), weight: initialWeight, squared: true

		m.add rule: (embersViolent(E, T)) >> violent(E, T), weight: initialWeight, squared: true
		m.add rule: (maxViolent(E, T)) >> violent(E, T), weight: initialWeight, squared: true

		m.add rule: (embersPopulation(E, T)) >> population(E, T), weight: initialWeight, squared: true
		m.add rule: (maxPopulation(E, T)) >> population(E, T), weight: initialWeight, squared: true
		m.add rule: (secondPopulation(E, T)) >> population(E, T), weight: initialWeight, squared: true

		for (String E : types) {
			UniqueID e = data.getUniqueID(E)
			for (String p : pops) {
				m.add rule: (type(A, e)) >> population(A, p), weight: initialWeight, squared: true
				m.add rule: (population(A, p)) >> type(A, e), weight: initialWeight, squared: true

				m.add rule: (type(A, e) & population(A, p)) >> suppress(A), weight: initialWeight, squared: true
				m.add rule: (type(A, e) & population(A, p)) >> ~suppress(A), weight: initialWeight, squared: true
			}
		}

		def countries = ["Argentina","Brazil","Chile","Colombia","Ecuador","El Salvador","Mexico","Paraguay","Uruguay","Venezuela"]

		for (String c : countries) {
			for (String p : pops) {
				m.add rule: (country(A, c)) >> population(A, p), weight: initialWeight, squared: true

				m.add rule: (country(A, c) & population(A, p)) >> suppress(A), weight: initialWeight, squared: true
				m.add rule: (country(A, c) & population(A, p)) >> ~suppress(A), weight: initialWeight, squared: true
			}
			for (String V : violents) {
				UniqueID v = data.getUniqueID(V)
				m.add rule: (country(A, c)) >> violent(A, v), weight: initialWeight, squared: true

				m.add rule: (country(A, c) & violent(A, v)) >> suppress(A), weight: initialWeight, squared: true
				m.add rule: (country(A, c) & violent(A, v)) >> ~suppress(A), weight: initialWeight, squared: true
			}
			for (String E : types) {
				UniqueID e = data.getUniqueID(E)
				m.add rule: (country(A, c)) >> type(A, e), weight: initialWeight, squared: true

				m.add rule: (country(A, c) & type(A, e)) >> suppress(A), weight: initialWeight, squared: true
				m.add rule: (country(A, c) & type(A, e)) >> ~suppress(A), weight: initialWeight, squared: true
			}
		}

		/**
		 * load data for training
		 */

		trainRead = new Partition(cb.getInt("partitions.trainread", -1));
		trainWrite = new Partition(cb.getInt("partitions.trainwrite", -1));
		trainLabel = new Partition(cb.getInt("partitions.trainlabel", -1));

		loadTrainingData(trainFile, trainRead, trainLabel);

		// populate
		Database readDB = data.getDatabase(trainLabel)

		Set<GroundAtom> typeAtoms = Queries.getAllAtoms(readDB, type)
		Set<GroundTerm> gsrEvents = new HashSet<GroundTerm>()
		for (GroundAtom atom : typeAtoms)
			gsrEvents.add(atom.getArguments()[0])

		readDB.close()

		Database writeDB = data.getDatabase(trainWrite)
		for (GroundTerm id : gsrEvents) {
			populate(writeDB, type, id, types)
			populate(writeDB, violent, id, violents)
			populate(writeDB, population, id, pops)

			RandomVariableAtom atom = writeDB.getAtom(suppress, id);
			atom.setValue(0.0);
			atom.commitToDB();
		}

		writeDB.close()

		log.info("Finished loading and populating")
	}

	/**
	 * runs weight learning
	 */
	public void learn() {
		def trainDB = data.getDatabase(trainWrite, trainRead)
		def labelsDB = data.getDatabase(trainLabel, [violent, population, type, suppress] as Set)
		//WeightLearningApplication wl = new MaxPseudoLikelihood(m, trainDB, labelsDB, cb)
		WeightLearningApplication wl = new MaxLikelihoodMPE(m, trainDB, labelsDB, cb)

		wl.learn()

		trainDB.close()
		labelsDB.close()
	}

	/**
	 * output model file
	 * @param filename output path and filename
	 */
	public void outputModel(String filename) {
		PSLModelLoader.outputModel(filename, m);
	}

	/**
	 * loads training instances from json file into PSL database
	 * @param filename location of input json file
	 * @param trainRead partition to store training data
	 * @param trainLabel partition to store ground truth labels
	 */
	private void loadTrainingData(String filename, Partition trainRead, Partition trainLabel) {
		FileInputStream fis = new FileInputStream(filename);
		Reader decoder = new InputStreamReader(fis, "UTF-8");
		BufferedReader reader = new BufferedReader(decoder);

		def readDB = data.getDatabase(trainRead);
		def labelDB = data.getDatabase(trainLabel);

		while (reader.ready()) {
			String line = reader.readLine();

			JSONObject object = new JSONObject(line);

			JSONObject gsr = object.getJSONObject("matched_gsr");

			jsonToPSL(object, readDB);

			String embersId = object.getString("embersId");
			String eventType = gsr.getString("eventType");
			String type = eventType.substring(0, 3)
			String violent = eventType.substring(4, 4);
			String population = gsr.getString("population");

			insertAtom(1.0, labelDB, typePred, embersId, type);
			insertAtom(1.0, labelDB, popPred, embersId, population);
			insertAtom(1.0, labelDB, violPred, embersId, violent);


			double quality = object.getJSONObject("match_score").getJSONObject("mean_score").getDouble("total_quality");

			if (quality < SUPPRESS_THRESHOLD)
				insertAtom(1.0, labelDB, suppressPred, embersId);
			else
				insertAtom(0.0, labelDB, suppressPred, embersId);

		}

		readDB.close();
		labelDB.close();

		reader.close();
		decoder.close();
		fis.close();
	}

	/**
	 * converts embers json warning into PSL predicates
	 * @param object json warning object
	 * @param db database to insert PSL predicates
	 */
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
	}

	/**
	 * Utility function to help compute max
	 * @param map
	 * @return
	 */
	private Map.Entry<String, Double> removeMax(Map<String, Double> map) {
		Map.Entry<String, Double> best = null;

		for (Map.Entry<String, Double> e : map.entrySet()) {
			if (best == null || e.getValue() > best.getValue())
				best = e;
		}

		map.remove(best.getKey());
		return best;
	}

	/**
	 * Utility function to insert facts into PSL database
	 * @param value
	 * @param db
	 * @param pred
	 * @param args
	 */
	private void insertAtom(Double value, Database db, Predicate pred, String ... args) {
		GroundTerm [] convertedArgs = Queries.convertArguments(db, pred, args);

		RandomVariableAtom atom = db.getAtom(pred, convertedArgs);
		atom.setValue(value);
		atom.commitToDB();
	}

	/**
	 * Utility function to populate possible inferred values into PSL write partition
	 * @param db
	 * @param p
	 * @param term
	 * @param targets
	 */
	private void populate(Database db, Predicate p, GroundTerm term, List<String> targets) {
		for (String i : targets) {
			GroundTerm [] convertedArgs = Queries.convertArguments(db, p, term, i);

			RandomVariableAtom atom = db.getAtom(p, convertedArgs);

			atom.setValue(0)
			atom.commitToDB()
		}
	}

	/**
	 * Runs the training. 
	 * Most parameters are set using psl.properties config file, but the 
	 * training file location and the output location are set via command 
	 * line arguments
	 * @param args
	 */
	static void main(String [] args) {
		if (args.length < 2)
			throw new IllegalArgumentException("Must be called with training file and output file");
		PSLRewriterTrainer trainer = new PSLRewriterTrainer(args[0]);
		trainer.learn();
		trainer.outputModel(args[1]);
	}
}
