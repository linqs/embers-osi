package edu.umd.cs.linqs.embers

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.ui.loading.InserterUtils
import edu.umd.cs.psl.util.database.Queries

/**
 * Runs periodically to setup gazetteer and other data for psl-rss prediction scripts.
 */
Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("rss")

String defaultPath = System.getProperty("java.io.tmpdir");
String dbPath = cb.getString("dbpath", defaultPath + File.separator);
String dbName = cb.getString("dbname", "psl");
String fullDBPath = dbPath + dbName;
/* Reinitializes the RDBMS to an empty Datastore */
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);

/* Initializes model (just for defining gazetteer) */
PSLModel m = new PSLModel(this, data)

/* Defines Gazetteer predicates */
m.add predicate: "OfficialName", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Alias", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Location_Type", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Population", types: [ArgumentType.UniqueID, ArgumentType.Integer]
m.add predicate: "LatLong", types: [ArgumentType.UniqueID, ArgumentType.Integer, ArgumentType.Integer]
m.add predicate: "Country", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Admin1", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Admin2", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "RefersTo", types: [ArgumentType.String, ArgumentType.UniqueID];

/* Parses gazetteer */
String auxDataPath = cb.getString("auxdatapath", "");
String gazetteerName = cb.getString("gazetteername", "");
String fullGazetteerPath = auxDataPath + gazetteerName;
String refersToFileName = cb.getString("referstoname", "");
String fullRefersToFilePath = auxDataPath + refersToFileName;

Partition gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));

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
	line = NormalizeText.stripAccents(line);
	String[] tokens = line.split(delim);
	insertRawArguments(db, OfficialName, tokens[keyIndex], tokens[officialNameIndex]);
	insertRawArguments(db, Alias, tokens[keyIndex], tokens[aliasIndex]);
	if (!tokens[optAliasIndexA].equals(""))
		insertRawArguments(db, Alias, tokens[keyIndex], tokens[optAliasIndexA]);
	if (!tokens[optAliasIndexB].equals(""))
		insertRawArguments(db, Alias, tokens[keyIndex], tokens[optAliasIndexB]);
	insertRawArguments(db, Location_Type, tokens[keyIndex], tokens[typeIndex]);
	insertRawArguments(db, Population, tokens[keyIndex], tokens[popIndex]);
	insertRawArguments(db, LatLong, tokens[keyIndex], tokens[latIndex], tokens[longIndex]);
	insertRawArguments(db, Country, tokens[keyIndex], tokens[countryIndex]);
	if (tokens.length > admin1Index && !tokens[admin1Index].equals("")) {
		insertRawArguments(db, Admin1, tokens[keyIndex], tokens[admin1Index]);
	}
	if (tokens.length > admin2Index && !tokens[admin2Index].equals("")) {
		insertRawArguments(db, Admin2, tokens[keyIndex], tokens[admin2Index]);
	}
}

db.close();

/* Loads normalized population info */
InserterUtils.loadDelimitedDataTruth(data.getInserter(RefersTo, gazPart), fullRefersToFilePath);

log.info("Inserted gazetteer into PSL database")

private void insertRawArguments(Database db, Predicate p, Object... rawArgs) {
	GroundTerm[] args = Queries.convertArguments(db, p, rawArgs);
	GroundAtom atom = db.getAtom(p, args);
	atom.setValue(1.0);
	if (atom instanceof RandomVariableAtom)
		atom.commitToDB();
}
