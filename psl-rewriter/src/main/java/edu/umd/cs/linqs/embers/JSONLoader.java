package edu.umd.cs.linqs.embers;

import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.model.argument.UniqueID;
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.predicate.Predicate;

/**
 * A start on generalizing json loading into PSL schemas. Currently unfinished and unused
 * @author Bert Huang
 *
 */
public class JSONLoader {

	private Database db;
	JSONObject json;

	public JSONLoader(Database db) {
		this.db = db;
	}

	public boolean setJson(String jsonString) {
		try {
			json = new JSONObject(jsonString);
			return true;
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * From a single json object, for key string, loads PSL hard truth values for
	 * any matching Predicate String pairs in predicateMap.
	 * E.g., if the predicateMap is a single pair (location, "loc"), and the input json
	 * is { "id":011, "loc":"Boston" }, then, the DataBase would get the entry:
	 * Location(011, "Boston")
	 *
	 * @param key
	 * @param predicateMap
	 */
	public void loadKeyValues(String key, Map<Predicate, String> predicateMap) {
		for (Map.Entry<Predicate, String> e : predicateMap.entrySet())
			loadKeyValues(key, e.getKey(), e.getValue());
	}

	public void loadKeyValues(String key, Predicate predicate, String predicateString) {
		UniqueID keyID = db.getUniqueID(key);
		try {
			String value = json.getString(predicateString);
			UniqueID valID = db.getUniqueID(value);
			RandomVariableAtom atom = (RandomVariableAtom) db.getAtom(predicate, keyID, valID);
			atom.setValue(1.0);
			atom.commitToDB();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	public void loadKeyList(String key, Predicate predicate, String listName) {
		UniqueID keyID = db.getUniqueID(key);

		try {
			JSONArray array = json.getJSONArray(listName);
			
			for (int i = 0; i < array.length(); i++) {
				JSONObject obj = array.getJSONObject(i);
				Iterator<String> keyIter = obj.keys();
				while (keyIter.hasNext()) {
					String k = keyIter.next();
					UniqueID kID = db.getUniqueID(k);
					String value = obj.getString(k);
					UniqueID valID = db.getUniqueID(value);
					RandomVariableAtom atom = (RandomVariableAtom) db.getAtom(predicate, keyID, kID, valID);
					atom.setValue(1.0);
					atom.commitToDB();			
				}
				
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}


}
