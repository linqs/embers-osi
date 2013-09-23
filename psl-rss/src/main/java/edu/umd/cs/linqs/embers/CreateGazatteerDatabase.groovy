package edu.umd.cs.linqs.embers

import org.json.JSONException
import org.json.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.application.inference.LazyMPEInference
import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.DatabaseQuery
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.evaluation.result.FullInferenceResult
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.groovy.PredicateConstraint
import edu.umd.cs.psl.model.Model
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.argument.Variable
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.QueryAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.ui.loading.InserterUtils
import edu.umd.cs.psl.util.database.Queries
import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.text.DateFormat
import java.text.SimpleDateFormat


String CONFIG_PREFIX = "rsslocationprocessor"

/* Key for result output directory */
String OUTPUT_DIR_KEY = CONFIG_PREFIX + ".outputdir"

Logger log = LoggerFactory.getLogger(this.class)
ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("rss")


String defaultPath = System.getProperty("java.io.tmpdir");
String baseDBPath = cb.getString("basedbpath", defaultPath);
String baseDBName = cb.getString("basedbname", "psl");
String fullBaseDBPath = baseDBPath + baseDBName;
String outputDirectory = cb.getString(OUTPUT_DIR_KEY, null);

String BLANK = "-"

DataStore data
Partition read
Partition write
Partition gazPart

int readPartNum = 0;
int writePartNum = 1;

HashMap<String, String> officialCountries;
HashMap<String, String> officialStates;

log.debug("Creating database {}", fullBaseDBPath)
data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullBaseDBPath, true), cb);
read = new Partition(readPartNum)
write = new Partition(writePartNum)
gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));

PSLModel m = new PSLModel(this, data);

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

officialCountries = new HashMap<String>();
officialStates = new HashMap<String>();


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
	insertRawArguments(db, OfficialCountry, country, officialCountries.get(country));
}
for (String state : officialStates.keySet()) {
	insertRawArguments(db, IsState, state)
	insertRawArguments(db, OfficialState, state, officialStates.get(state));
}
db.close();

log.info("Inserted gazetteer into PSL database")
System.gc();




private void insertRawArguments(Database db, Predicate p, Object... rawArgs) {
	GroundTerm[] args = Queries.convertArguments(db, p, rawArgs);
	GroundAtom atom = db.getAtom(p, args);
	if (atom instanceof RandomVariableAtom) {
		RandomVariableAtom rv = (RandomVariableAtom) atom;
		rv.setValue(1.0);
		rv.commitToDB();
	}
}