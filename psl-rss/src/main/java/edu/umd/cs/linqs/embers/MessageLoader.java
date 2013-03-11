package edu.umd.cs.linqs.embers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umd.cs.linqs.embers.json.Entity;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.IntegerAttribute;
import edu.umd.cs.psl.model.argument.StringAttribute;
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.predicate.Predicate;

/**
 * Utility object for loading EMBERS JSON feed into PSL
 * Currently loads entities extracted by BASIS named entity extraction
 * 
 * @author Bert Huang <bert@cs.umd.edu>
 *
 */
public class MessageLoader {

	private JSONObject json;
	private List<Entity> entities;
	private String embersId;
	private String language;

	public MessageLoader(String filename) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));

			String line = reader.readLine();
			json = new JSONObject(line);

			JSONArray entitiesArray = json.getJSONObject("BasisEnrichment").getJSONArray("entities");

			embersId = json.getString("embersId");
			language = json.getJSONObject("BasisEnrichment").getString("language");


			entities = new ArrayList<Entity>(entitiesArray.length());

			for (int i = 0 ; i < entitiesArray.length(); i++) {
				entities.add(i, new Entity(entitiesArray.getJSONObject(i)));
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public MessageLoader(JSONObject json) {
		try {
			JSONArray entitiesArray = json.getJSONObject("BasisEnrichment").getJSONArray("entities");

			embersId = json.getString("embersId");
			language = json.getJSONObject("BasisEnrichment").getString("language");


			entities = new ArrayList<Entity>(entitiesArray.length());

			for (int i = 0 ; i < entitiesArray.length(); i++) {
				entities.add(i, new Entity(entitiesArray.getJSONObject(i)));
			}

			this.json = json;

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Inserts entities extracted by BASIS named entity extraction into PSL 
	 * database
	 * @param db
	 * @param entity
	 */
	public void insertAllEntities(Database db, Predicate entity) {
		for (Entity e : entities) {
			GroundTerm [] arguments = new GroundTerm[4];
			arguments[0] = db.getUniqueID(embersId);
			arguments[1] = new StringAttribute(e.getExpr().toLowerCase());
			arguments[2] = new StringAttribute(e.getNeType());
			arguments[3] = new IntegerAttribute(e.getOffset());
			RandomVariableAtom atom = (RandomVariableAtom) db.getAtom(entity, arguments);
			atom.setValue(1.0);
			db.commit(atom);
		}
	}

	/**
	 * Inserts entities extracted by BASIS named entity extraction into PSL 
	 * database
	 * @param db
	 * @param entity
	 */
	public void insertAllTokens(Database db, Predicate entity) {
		for (Entity e : entities) {
			String [] tokens = e.getExpr().toLowerCase().split("\\s+");
			for (int i = 0; i < tokens.length; i++) {
				StringBuilder sb = new StringBuilder();
				sb.append(tokens[i]);
				for (int j = i+1; j < tokens.length; j++) {
					//System.out.println("Adding " + sb.toString());
					sb.append(" " + tokens[j]);
					GroundTerm [] arguments = new GroundTerm[4];
					arguments[0] = db.getUniqueID(embersId);
					arguments[1] = new StringAttribute(sb.toString());
					arguments[2] = new StringAttribute(e.getNeType());
					arguments[3] = new IntegerAttribute(e.getOffset());
					RandomVariableAtom atom = (RandomVariableAtom) db.getAtom(entity, arguments);
					atom.setValue(1.0);
					db.commit(atom);
				}
			}
		}
	}

	public String [] getEntityNames() {
		String [] vec = new String[entities.size()];

		for (int i = 0; i < entities.size(); i++) {
			vec[i] = entities.get(i).getExpr();
		}
		return vec;
	}

	/**
	 * Inserts language of article into database
	 * @param db
	 * @param writtenIn
	 */
	public void insertWrittenIn(Database db, Predicate writtenIn) {
		GroundTerm [] arguments = new GroundTerm[2];
		arguments[0] = db.getUniqueID(embersId);
		arguments[1] = new StringAttribute(language);
		RandomVariableAtom atom = (RandomVariableAtom) db.getAtom(writtenIn, arguments);
		atom.setValue(1.0);
		db.commit(atom);
	}

	public void enrichGeocode(String country, String state, String city) throws JSONException {
//		JSONObject psl = new JSONObject();
//		JSONObject geocode = new JSONObject();
//		geocode.put("country", country);
//		geocode.put("state", state);
//		geocode.put("city", city);
//		psl.put("pslGeocode", geocode);
//		json.put("pslEnrichment", psl);
		
		
		JSONObject embersGeoCode = new JSONObject();
		embersGeoCode.put("country", country);
		embersGeoCode.put("admin1", state);
		embersGeoCode.put("city", city);
		json.put("embersGeoCode", embersGeoCode);
	}

	public void writeOut(String outDir) throws JSONException, IOException {
		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(
				new File(outDir, embersId)), "utf-8");
		json.write(writer);
		writer.flush();
		writer.close();
	}

	public JSONObject getJSONObject() {
		return json;
	}


	public static void main(String [] args) {
		@SuppressWarnings("unused")
		MessageLoader ml = new MessageLoader("messages/b72bd8382210aff316127f05ebbac3dae78c96c3");
	}

}
