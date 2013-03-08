package edu.umd.cs.linqs.embers

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
import edu.umd.cs.psl.model.argument.UniqueID;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.atom.QueryAtom
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

	private DataStore data
	private Partition read
	private Partition write
	private Partition gazPart

	private PSLModel m

	public RSSLocationProcessor() {
		/* Reinitializes the RDBMS to an empty Datastore */
		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, false), cb);
		read = new Partition(0)
		write = new Partition(1)
		gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));
		m = new PSLModel(this, data)
		
		

		m.add predicate: "Entity", types: [ArgumentType.UniqueID, ArgumentType.String, ArgumentType.String, ArgumentType.Integer]
		m.add predicate: "WrittenIn", types: [ArgumentType.UniqueID, ArgumentType.String]
		m.add predicate: "PSL_Location", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		m.add predicate: "ArticleCountry", types: [ArgumentType.UniqueID, ArgumentType.String]
		
		m.add PredicateConstraint.PartialFunctional, on: ArticleCountry
		m.add PredicateConstraint.PartialFunctional, on: PSL_Location

		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Alias(LocID, Location) & RefersTo(Location, LocID) & ArticleCountry(Article, C) & Country(LocID, C)) >> PSL_Location(Article, LocID), weight: 1.0, squared: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Alias(LocID, Location) & RefersTo(Location, LocID) & Country(LocID, C)) >> ArticleCountry(Article, C), weight: 1.0, squared: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Country(LocID, Location) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Admin1(LocID, Location) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 0.5, squared: true
		m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Admin2(LocID, Location) & RefersTo(Location, LocID)) >> PSL_Location(Article, LocID), weight: 0.5, squared: true
	}


	@Override
	public JSONObject process(JSONObject json) {
		data.deletePartition(read)
		data.deletePartition(write)
		Database db = data.getDatabase(read)

		MessageLoader loader = new MessageLoader(json)

		loader.insertAllEntities(db, Entity)
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
		String predictedCountry = "";
		String predictedCity;
		String predictedState;
		UniqueID predictedLocation;
		double countryScore = 0.0;
		double locationScore = 0.0;


		for (GroundAtom atom : Queries.getAllAtoms(db, ArticleCountry)) {
			log.debug(atom.toString() + " : " + atom.getValue());
			if (atom.getValue() > countryScore) {
				predictedCountry = atom.getArguments()[1].getValue();
				countryScore = atom.getValue();
			}
		}

		for (GroundAtom atom : Queries.getAllAtoms(db, PSL_Location)) {
			log.debug(atom.toString() + " : " + atom.getValue());
			if (atom.getValue() > locationScore) {
				predictedLocation = atom.getArguments()[1];
				locationScore = atom.getValue();
			}
		}


		// look up city and state for location
		Variable var = new Variable("var")
		if (predictedLocation == null) {
			predictedState = ""
			predictedCity = ""
		} else {
			def query = new DatabaseQuery(new QueryAtom(OfficialName, Queries.convertArguments(db, OfficialName, predictedLocation, var)))
			ResultList list = db.executeQuery(query)
			predictedCity = list.get(0)[0].getValue()
			query = new DatabaseQuery(new QueryAtom(Admin1, Queries.convertArguments(db, Admin1, predictedLocation, var)))
			list = db.executeQuery(query)
			predictedState = list.get(0)[0].getValue()
		}
		db.close()

		log.debug("Location is {}", predictedLocation)
		log.debug("Country is {}", predictedCountry)
		log.debug("City is {}", predictedCity)
		log.debug("State is {}", predictedState)

		
		loader.enrichGeocode(predictedCountry, predictedState, predictedCity)
		
		if (outputDirectory != null)
			loader.writeOut(outputDirectory);

		return loader.getJSONObject();
	}
}
