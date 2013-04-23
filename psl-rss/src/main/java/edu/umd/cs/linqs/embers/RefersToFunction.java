package edu.umd.cs.linqs.embers;

import edu.umd.cs.psl.database.DatabaseQuery;
import edu.umd.cs.psl.database.ReadOnlyDatabase;
import edu.umd.cs.psl.database.ResultList;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.IntegerAttribute;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.atom.QueryAtom;
import edu.umd.cs.psl.model.formula.Conjunction;
import edu.umd.cs.psl.model.function.ExternalFunction;
import edu.umd.cs.psl.model.predicate.Predicate;
import edu.umd.cs.psl.model.predicate.PredicateFactory;

/**
 * Normalizes population over all locations with a given alias.
 * 
 * @author Stephen Bach <bach@cs.umd.edu>
 */
public class RefersToFunction implements ExternalFunction {

	@Override
	public double getValue(ReadOnlyDatabase db, GroundTerm... args) {
		PredicateFactory pf = PredicateFactory.getFactory();
		Predicate Alias = pf.getPredicate("Alias");
		Predicate Population = pf.getPredicate("Population");
		Variable LocID = new Variable("LocID");
		Variable P = new Variable("P");
		DatabaseQuery query = new DatabaseQuery(new Conjunction(new QueryAtom(Alias, LocID, args[0]), new QueryAtom(Population, LocID, P))); 
		query.getProjectionSubset().add(LocID);
		query.getProjectionSubset().add(P);
		ResultList results = db.executeQuery(query);
		
		double numerator = 0;
		double denominator = 0;
		
		for (int i = 0; i < results.size(); i++) {
			denominator += ((IntegerAttribute) results.get(i, P)).getValue();
			if (results.get(i, LocID).equals(args[1]))
				numerator = ((IntegerAttribute) results.get(i, P)).getValue();
		}
		
		return numerator / denominator;
	}

	@Override
	public int getArity() {
		return 2;
	}

	@Override
	public ArgumentType[] getArgumentTypes() {
		return new ArgumentType[] {ArgumentType.String, ArgumentType.UniqueID};
	}

}
