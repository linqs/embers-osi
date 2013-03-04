package edu.umd.cs.linqs.embers

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.application.inference.LazyMPEInference
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.evaluation.result.FullInferenceResult;
import edu.umd.cs.psl.groovy.PSLModel;
import edu.umd.cs.psl.groovy.PredicateConstraint;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.util.database.Queries;

/**
 * Runs periodically to setup gazetteer and other data for prediction scripts.
 */
Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("rss")

String defaultPath = System.getProperty("java.io.tmpdir");
String dbPath = cb.getString("dbpath", defaultPath);
String dbName = cb.getString("dbame", "psl");
String fullDBPath = dbPath + dbName;
/* Reinitializes the RDBMS to an empty Datastore */
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, false), cb);
Partition read = new Partition(0)
Partition write = new Partition(1)
data.deletePartition(read)
data.deletePartition(write)

/* Initializes model (just for defining gazetteer) */
PSLModel m = new PSLModel(this, data)

/* Defines Gazetteer predicates */
//m.add predicate: "AlsoKnownAs", types: [ArgumentType.UniqueID, ArgumentType.String]
//m.add predicate: "LatLong", types: [ArgumentType.UniqueID, ArgumentType.Double, ArgumentType.Double]
//m.add predicate: "Cat", types: [ArgumentType.UniqueID]
//m.add predicate: "Country", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
//m.add predicate: "Admin1", types: [ArgumentType.UniqueID, ArgumentType.String]
//m.add predicate: "Admin2", types: [ArgumentType.UniqueID, ArgumentType.String]

m.add predicate: "Entity", types: [ArgumentType.UniqueID, ArgumentType.String, ArgumentType.String, ArgumentType.Integer]
m.add predicate: "WrittenIn", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add predicate: "PSL_Location", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
m.add predicate: "ArticleCountry", types: [ArgumentType.UniqueID, ArgumentType.String]
m.add function: "RefersTo", implementation: new RefersTo()

Partition gazPart = new Partition(cb.getInt("partitions.gazetteer", -1));

Database db = data.getDatabase(read)

MessageLoader loader = new MessageLoader("messages/2b4304a6f1621feb4a1d902a79ba4c372694e946")

loader.insertAllEntities(db, Entity)
loader.insertWrittenIn(db, WrittenIn)
db.close()

toClose = [Entity, WrittenIn, Alias, Country, LatLong, Cat, Admin1, Admin2] as Set

db = data.getDatabase(write, toClose, read, gazPart)

m.add PredicateConstraint.PartialFunctional, on: ArticleCountry
m.add PredicateConstraint.PartialFunctional, on: PSL_Location

m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Alias(LocID, Location) & RefersTo(Location, LocID) & ArticleCountry(Article, C) & Country(LocID, C)) >> PSL_Location(Article, LocID), weight: 1.0, squared: true
m.add rule: (Entity(Article, Location, "LOCATION", Offset) & Alias(LocID, Location) & RefersTo(Location, LocID) & Country(LocID, C)) >> ArticleCountry(Article, C), weight: 1.0, squared: true

LazyMPEInference mpe = new LazyMPEInference(m, db, cb)
FullInferenceResult result = mpe.mpeInference()
System.out.println("Total incompatibility: " + result.getTotalWeightedIncompatibility());
System.out.println("Infeasibility norm: " + result.getInfeasibilityNorm());

for (GroundAtom atom : Queries.getAllAtoms(db, ArticleCountry))
	System.out.println(atom.toString() + " : " + atom.getValue());

db.close()


