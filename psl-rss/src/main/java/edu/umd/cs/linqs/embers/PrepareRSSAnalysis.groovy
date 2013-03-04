package edu.umd.cs.linqs.embers

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Partition;
import edu.umd.cs.psl.database.loading.Inserter;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.groovy.PSLModel;
import edu.umd.cs.psl.model.argument.ArgumentType;

/**
 * Runs periodically to setup gazetteer and other data for psl-rss prediction scripts.
 */
Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("rss")

String defaultPath = System.getProperty("java.io.tmpdir");
String dbPath = cb.getString("dbpath", defaultPath + File.separator);
String dbName = cb.getString("dbame", "psl");
String fullDBPath = dbPath + dbName;
/* Reinitializes the RDBMS to an empty Datastore */
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);

/* Initializes model (just for defining gazetteer) */
PSLModel m = new PSLModel(this, data)

/* Defines Gazetteer predicates */
m.add predicate: "Alias", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Location_Type", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Population", types: [ArgumentType.UniqueID, ArgumentType.Integer]
m.add predicate: "LatLong", types: [ArgumentType.UniqueID, ArgumentType.Integer, ArgumentType.Integer]
m.add predicate: "Country", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Admin1", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Admin2", types: [ArgumentType.UniqueID, ArgumentType.String]

/* Parses gazetteer */
String auxDataPath = cb.getString("auxdatapath", "");
String gazetteerName = cb.getString("gazetteername", "");
String fullGazetteerPath = auxDataPath + gazetteerName;

Partition gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));
Inserter aliasInsert = data.getInserter(Alias, gazPart);
Inserter typeInsert = data.getInserter(Location_Type, gazPart);
Inserter popInsert = data.getInserter(Population, gazPart);
Inserter latLongInsert = data.getInserter(LatLong, gazPart);
Inserter countryInsert = data.getInserter(Country, gazPart);
Inserter admin1Insert = data.getInserter(Admin1, gazPart);
Inserter admin2Insert = data.getInserter(Admin2, gazPart);

BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fullGazetteerPath), "utf-8"));
String line;
String delim = cb.getString("gazetteerdelim", "\t");
int keyIndex = 0;
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
	String[] tokens = line.split(delim);
	aliasInsert.insert(tokens[keyIndex], tokens[aliasIndex]);
	if (!tokens[optAliasIndexA].equals(""))
		aliasInsert.insert(tokens[keyIndex], tokens[optAliasIndexA]);
	if (!tokens[optAliasIndexB].equals(""))
		aliasInsert.insert(tokens[keyIndex], tokens[optAliasIndexB]);
	typeInsert.insert(tokens[keyIndex], tokens[typeIndex]);
	popInsert.insert(tokens[keyIndex], tokens[popIndex]);
	latLongInsert.insert(tokens[keyIndex], tokens[latIndex], tokens[longIndex]);
	countryInsert.insert(tokens[keyIndex], tokens[countryIndex]);
	if (tokens.length > admin1Index && !tokens[admin1Index].equals(""))
		admin1Insert.insert(tokens[keyIndex], tokens[admin1Index]);
	if (tokens.length > admin2Index && !tokens[admin2Index].equals(""))
		admin2Insert.insert(tokens[keyIndex], tokens[admin2Index]);
}
