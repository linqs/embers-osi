package edu.umd.cs.linqs.embers

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.json.JSONException;
import org.json.JSONObject
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.application.inference.LazyMPEInference
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.DatabaseQuery
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.evaluation.result.FullInferenceResult;
import edu.umd.cs.psl.groovy.PSLModel;
import edu.umd.cs.psl.groovy.PredicateConstraint;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.UniqueID;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.atom.QueryAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.predicate.Predicate;
import edu.umd.cs.psl.ui.loading.InserterUtils;
import edu.umd.cs.psl.util.database.Queries;

class RSSLocationProcessor implements JSONProcessor {

	private final String CONFIG_PREFIX = "rsslocationprocessor"

	/* Key for result output directory */
	private final String OUTPUT_DIR_KEY = CONFIG_PREFIX + ".outputdir"

	private final Logger log = LoggerFactory.getLogger(this.class)
	private final ConfigManager cm = ConfigManager.getManager()
	private final ConfigBundle cb = cm.getBundle("rss")


	private final String defaultPath = System.getProperty("java.io.tmpdir");
	private final String dbPath = cb.getString("dbpath", defaultPath);
	private String dbName = cb.getString("dbname", "psl");
	private String fullDBPath = dbPath + dbName;
	private final String outputDirectory = cb.getString(OUTPUT_DIR_KEY, null);

	private final String BLANK = "-"

	private DataStore data
	private Partition read
	private Partition write
	private Partition gazPart

	private PSLModel m

	private final int readPartNum = 0;
	private final int writePartNum = 1;

	HashMap<String, String> officialCountries;
	HashMap<String, String> officialStates;

	public RSSLocationProcessor() {
		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);
		read = new Partition(readPartNum)
		write = new Partition(writePartNum)
		gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));

		officialCountries = new HashMap<String>();
		officialStates = new HashMap<String>();

		m = new PSLModel(this, data)

		doPrepareRSSAnalysis();

		m.add predicate: "Entity", types: [ArgumentType.UniqueID, ArgumentType.String, ArgumentType.String, ArgumentType.Integer]
		m.add predicate: "WrittenIn", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "PSL_Location", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "ArticleCountry", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "ArticleState", types: [ArgumentType.UniqueID, ArgumentType.String]

		m.add PredicateConstraint.PartialFunctional, on: ArticleCountry
		m.add PredicateConstraint.PartialFunctional, on: ArticleState
		m.add PredicateConstraint.PartialFunctional, on: PSL_Location

		m.add rule: (Entity(Article, Location, "ORGANIZATION", Offset) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "PERSON", Offset) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 0.5, squared: true

		m.add rule: (Entity(Article, Location, "ORGANIZATION", Offset) & IsCountry(Location)) >> ArticleCountry(Article, Location), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "PERSON", Offset) & IsCountry(Location)) >> ArticleCountry(Article, Location), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "ORGANIZATION", Offset) & IsState(Location)) >> ArticleState(Article, Location), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "PERSON", Offset) & IsState(Location)) >> ArticleState(Article, Location), weight: 0.5, squared: true


		//m.add rule: (Entity(Article, Location, "LOCATION", Offset) & RefersTo(Location, LocID) & Neighbor(LocID, N)) >> PSL_Location(Article, N), weight: 0.3, squared: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 1.0, squared: true
		m.add rule: (Entity(Article, C, "LOCATION", Offset) & IsCountry(C)) >> ArticleCountry(Article, C), weight: 1.0, squared: true
		m.add rule: (Entity(Article, S, "LOCATION", Offset) & IsState(S)) >> ArticleState(Article, S), weight: 1.0, squared: true

		m.add rule: (PSL_Location(Article, LocID) & Country(LocID, C)) >> ArticleCountry(Article, C), constraint: true
		m.add rule: (PSL_Location(Article, LocID) & Admin1(LocID, S)) >> ArticleState(Article, S), constraint: true
		//m.add rule: (PSL_Location(Article, LocID) & Admin2(LocID, S)) >> ArticleState(Article, S), weight: 1.0, squared: true

		// blacklisting
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & RefersTo(Location, LocID) & Blacklist(Location)) >> ~PSL_Location(Article, LocID), constraint: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Blacklist(Location)) >> ~ArticleState(Article, LocID), constraint: true

		// trim all non-latin american countries if the article does not contain the country
		//		m.add rule: (PSL_Location(Article, LocID) & Country(LocID, C) & ArticleCountry(Article, C)) >> Entity(Article, C, "LOCATION", Offset), weight: 10.0, squared: true
	}

	/**
	 * 
	 */
	private void doPrepareRSSAnalysis() {
		/* Defines Gazetteer predicates */
		m.add predicate: "OfficialName", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "Alias", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "Country", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "Admin1", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "RefersTo", types: [ArgumentType.String, ArgumentType.UniqueID]
		m.add predicate: "IsCountry", types: [ArgumentType.String]
		m.add predicate: "IsState", types: [ArgumentType.String]
		m.add predicate: "OfficialCountry", types: [ArgumentType.String, ArgumentType.String]
		m.add predicate: "OfficialState", types: [ArgumentType.String, ArgumentType.String]
		m.add predicate: "Blacklist", types: [ArgumentType.String]

		/* Parses gazetteer */
		String auxDataPath = cb.getString("auxdatapath", "");
		String gazetteerName = cb.getString("gazetteername", "");
		String fullGazetteerPath = auxDataPath + gazetteerName;
		String refersToFileName = cb.getString("referstoname", "");
		String neighborFileName = cb.getString("neighborname", "");
		String fullRefersToFilePath = auxDataPath + refersToFileName;
		String fullNeighborFilePath = auxDataPath + neighborFileName;
		String blacklistFileName = cb.getString("blacklistname", "");
		String blacklistFilePath = auxDataPath + blacklistFileName;

		/* Loads normalized population info */
		InserterUtils.loadDelimitedDataTruth(data.getInserter(RefersTo, gazPart), fullRefersToFilePath);

		/* Loads location blacklist of common non-Latin American location strings */
		InserterUtils.loadDelimitedData(data.getInserter(Blacklist, gazPart), blacklistFilePath);

		Database db = data.getDatabase(gazPart);

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fullGazetteerPath), "utf-8"));
		String line;
		String delim = cb.getString("gazetteerdelim", "\t");
		int keyIndex = 0;
		/* Uses first name as both official name and an alias */
		int officialNameIndex = 1;
		int aliasIndex = 1;
		int optAliasIndexA = 2;
		int optAliasIndexB = 3;
		int typeIndex = 4;
		int popIndex = 5;
		int latIndex = 6;
		int longIndex = 7;
		int countryIndex = 8;
		int admin1Index = 9;
		int admin2Index = 10;


		while (line = reader.readLine()) {
			String[] rawTokens = line.split(delim);
			line = NormalizeText.stripAccents(line).toLowerCase();
			String[] tokens = line.split(delim);
			insertRawArguments(db, OfficialName, tokens[keyIndex], rawTokens[officialNameIndex]);
			insertRawArguments(db, Alias, tokens[keyIndex], tokens[aliasIndex]);
			if (!tokens[optAliasIndexA].equals(""))
				insertRawArguments(db, Alias, tokens[keyIndex], tokens[optAliasIndexA]);
			if (!tokens[optAliasIndexB].equals(""))
				insertRawArguments(db, Alias, tokens[keyIndex], tokens[optAliasIndexB]);
			insertRawArguments(db, Country, tokens[keyIndex], tokens[countryIndex]);
			if (tokens.length > admin1Index && !tokens[admin1Index].equals("")) {
				insertRawArguments(db, Admin1, tokens[keyIndex], tokens[admin1Index]);
				officialStates.put(tokens[admin1Index], rawTokens[admin1Index])
			}

			officialCountries.put(tokens[countryIndex], rawTokens[countryIndex])
		}

		for (String country : officialCountries.keySet()) {
			insertRawArguments(db, IsCountry, country)
		}
		for (String state : officialStates.keySet()) {
			insertRawArguments(db, IsState, state)
		}
		db.close();

		log.info("Inserted gazetteer into PSL database")
	}


	private void insertRawArguments(Database db, Predicate p, Object... rawArgs) {
		GroundTerm[] args = Queries.convertArguments(db, p, rawArgs);
		GroundAtom atom = db.getAtom(p, args);
		atom.setValue(1.0);
		if (atom instanceof RandomVariableAtom)
			atom.commitToDB();
	}

	@Override
	public String process(String jsonString) {
		try {
			JSONObject json = new JSONObject(jsonString);
			return process(json).toString();
		}
		catch (JSONException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public JSONObject process(JSONObject json) {
		data.deletePartition(read)
		data.deletePartition(write)
		Database db = data.getDatabase(read)

		MessageLoader loader = new MessageLoader(json)

		loader.insertAllEntities(db, Entity)
		loader.insertAllTokens(db, Entity)
		loader.insertWrittenIn(db, WrittenIn)
		db.close()

		def toClose = [Entity, WrittenIn, Alias, Population, Country, LatLong, Cat, Admin1, Admin2, RefersTo] as Set

		db = data.getDatabase(write, toClose, read, gazPart)

		LazyMPEInference mpe = new LazyMPEInference(m, db, cb)
		FullInferenceResult result = mpe.mpeInference()
		mpe.close()
		log.debug("Total incompatibility: " + result.getTotalWeightedIncompatibility());
		log.debug("Infeasibility norm: " + result.getInfeasibilityNorm());

		/**
		 * Compute predicted country, state, and city
		 */
		String predictedCountry = BLANK;
		String predictedCity = BLANK;
		String predictedState = BLANK;
		UniqueID predictedLocation;
		double countryScore = 0.0;
		double locationScore = 0.0;
		double stateScore = 0.0;


		for (GroundAtom atom : Queries.getAllAtoms(db, ArticleCountry)) {
			log.debug(atom.toString() + " : " + atom.getValue());
			if (atom.getValue() > countryScore) {
				predictedCountry = atom.getArguments()[1].getValue();
				countryScore = atom.getValue();
			}
		}

		for (GroundAtom atom : Queries.getAllAtoms(db, ArticleState)) {
			log.debug(atom.toString() + " : " + atom.getValue());
			if (atom.getValue() > stateScore) {
				predictedState = atom.getArguments()[1].getValue();
				stateScore = atom.getValue();
			}
		}

		for (GroundAtom atom : Queries.getAllAtoms(db, PSL_Location)) {
			//log.trace(atom.toString() + " : " + atom.getValue());
			if (atom.getValue() > locationScore) {
				predictedLocation = atom.getArguments()[1];
				locationScore = atom.getValue();
			}
		}


		// look up city and state for location
		if (predictedLocation == null) {
			predictedCity = BLANK
		} else {
			Variable var = new Variable("var")
			def query = new DatabaseQuery(new QueryAtom(OfficialName, Queries.convertArguments(db, OfficialName, predictedLocation, var)))
			ResultList list = db.executeQuery(query)
			predictedCity = list.get(0)[0].getValue()

			// look up state and country
			query = new DatabaseQuery(new QueryAtom(Admin1, Queries.convertArguments(db, Admin1, predictedLocation, var)))
			list = db.executeQuery(query)
			if (list.size() > 0)
				predictedState = list.get(0)[0].getValue()
			else
				predictedState = BLANK

			query = new DatabaseQuery(new QueryAtom(Country, Queries.convertArguments(db, Country, predictedLocation, var)))
			list = db.executeQuery(query)
			if (list.size() > 0)
				predictedCountry = list.get(0)[0].getValue()
			else
				predictedCountry = BLANK

		}

		if (!predictedState.equals(BLANK))
			predictedState = officialStates.get(predictedState)
		if (!predictedCountry.equals(BLANK))
			predictedCountry = officialCountries.get(predictedCountry)


		db.close()


		log.debug("Location is {}", predictedLocation)
		log.debug("Country is {}", predictedCountry)
		log.debug("City is {}", predictedCity)
		log.debug("State is {}", predictedState)

		if (predictedLocation == null) {
			log.debug("No location predicted. Entities were: {}", Arrays.asList(loader.getEntityNames()))
		}

		loader.enrichGeocode(predictedCountry, predictedState, predictedCity)

		if (outputDirectory != null)
			loader.writeOut(outputDirectory);

		return loader.getJSONObject();
	}

	public static void main(String [] args) {
		RSSLocationProcessor processor = new RSSLocationProcessor();

		//		while (true) {
		String filename = "aux_data/januaryGSRGeoCode.json";
		//			String filename = "aux_data/rss-content-enriched-2012-12-03-12-36-41.txt";
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));

		while (reader.ready()) {
			processor.process(reader.readLine())
		}

		ConfigBundle config = ConfigManager.getManager().getBundle("rss");

		String resultsPath = "./results"; //config.getString("enrichedpath", "");

		String gsrPath = config.getString("auxdatapath", "");
		String gsrFile = config.getString("gsr", "");
		String fullGSRPath = gsrPath + gsrFile;

		ResultsComparator eval = new ResultsComparator(resultsPath, fullGSRPath);
		eval.printEvalution(System.out);

	}
}
