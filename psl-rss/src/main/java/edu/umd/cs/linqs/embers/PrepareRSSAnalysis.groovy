package edu.umd.cs.linqs.embers

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.groovy.PSLModel;
import edu.umd.cs.psl.model.argument.ArgumentType;

/**
 * Runs periodically to setup gazetteer and other data for prediction scripts.
 */
Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("rss")

String defaultPath = System.getProperty("java.io.tmpdir");
String dbPath = cb.getString("dbpath", defaultPath);
String dbName = db.getString("dbame", "psl");
String fullDBPath = dbPath + dbName;
/* Reinitializes the RDBMS to an empty Datastore */
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb);

/* Initializes model (just for defining gazetteer) */
PSLModel m = new PSLModel(this, data)

/* Defines Gazetteer predicates */
m.add predicate: "AlsoKnownAs", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "LatLong", types: [ArgumentType.UniqueID, ArgumentType.Double, ArgumentType.Double]
m.add predicate: "Cat", types: [ArgumentType.UniqueID]
m.add predicate: "Country", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
m.add predicate: "Admin1", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "Admin2", types: [ArgumentType.UniqueID, ArgumentType.String]

