package edu.umd.cs.linqs.embers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umd.cs.linqs.embers.json.Entity;
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;

/**
 * Measures accuracy of PSL enrichment of messages according to a GSR file. 
 * 
 * @author Stephen Bach <bach@cs.umd.edu>
 */
public class ResultsEvaluator {

	private Map<Integer, gsrEntry> gsr;
	private String resultsPath;

	public ResultsEvaluator(String resultsPath, String gsrPath) throws IOException {
		gsr = parseGSR(gsrPath);
		this.resultsPath = resultsPath;
	}

	private Map<Integer, gsrEntry> parseGSR(String gsrPath) throws IOException {
		Map<Integer, gsrEntry> gsrMap = new HashMap<Integer, ResultsEvaluator.gsrEntry>();

		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(gsrPath), "utf-8"));
		String line;
		gsrEntry entry;

		/* Column map */
		int idIndex = 0;
		int countryIndex = 1;
		int stateIndex = 2;
		int cityIndex = 3;
		int codeIndex = 4;
		int typeIndex = 5;
		int populationIndex = 6;

		/* Eats the header */
		line = reader.readLine();

		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split(",");

			entry = new gsrEntry();

			entry.id = Integer.parseInt(tokens[idIndex]);
			entry.country = NormalizeText.stripAccents(tokens[countryIndex]);
			entry.state = NormalizeText.stripAccents(tokens[stateIndex]);
			entry.city = NormalizeText.stripAccents(tokens[cityIndex]);
			entry.code = Integer.parseInt(tokens[codeIndex]);
			entry.type = tokens[typeIndex];
			if (tokens.length > populationIndex)
				entry.population = tokens[populationIndex];
			else
				entry.population = "";

			gsrMap.put(entry.id, entry);
		}

		reader.close();

		return gsrMap;
	}

	public void printEvalution(PrintStream out) throws IOException, JSONException {
		int totalResults = 0;
		int totalCorrect = 0;
		int correctCountries = 0;
		int correctStates = 0;
		int correctCities = 0;

		int embersCorrect = 0;
		int embersCountries = 0;
		int embersStates = 0;
		int embersCities = 0;

		BufferedReader reader;

		/* Gets list of results files */
		File resultsDir = new File(resultsPath);
		File[] resultsFiles = resultsDir.listFiles();

		/* Scores each file */
		for (File resultsFile : resultsFiles) {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(resultsFile.getAbsolutePath()), "utf-8"));		
			String line = reader.readLine();

			JSONObject json = new JSONObject(line);
			JSONObject pslEnrichment = json.getJSONObject("pslEnrichment");
			JSONObject pslGeocode = pslEnrichment.getJSONObject("pslGeocode");

			int eventID = json.getInt("eventId"); 
			String country = pslGeocode.getString("country");
			String state = pslGeocode.getString("state");
			String city = pslGeocode.getString("city");

			JSONObject embersGeocode = json.getJSONObject("embersGeoCode");
			String embersCountry = embersGeocode.getString("country");
			String embersState = embersGeocode.getString("admin1");
			String embersCity= embersGeocode.getString("city");

			gsrEntry entry = gsr.get(eventID);
			if (entry == null) {
				System.out.println("WARN: No GSR entry for event ID: " + eventID + ". Skipping.");
				continue;
			}

			System.out.println("\n\nEvent ID: " + entry.id);
			System.out.println("EMBERS country: " + embersCountry + ", PSL country: " + country + ", true country: " + entry.country);
			System.out.println("EMBERS state: " + embersState + ", PSL state: " + state + ", true state: " + entry.state);
			System.out.println("EMBERS city: " + embersCity+ ", PSL city: " + city + ", true city: " + entry.city);

			boolean allCorrect = true;
			if (country.toLowerCase().equals(entry.country.toLowerCase()))
				correctCountries++;
			else
				allCorrect = false;
			if (state.toLowerCase().equals(entry.state.toLowerCase()))
				correctStates++;
			else
				allCorrect = false;
			if (city.toLowerCase().equals(entry.city.toLowerCase()))
				correctCities++;
			else
				allCorrect = false;

			if (allCorrect)
				totalCorrect++;



			allCorrect = true;
			if (embersCountry.toLowerCase().equals(entry.country.toLowerCase()))
				embersCountries++;
			else
				allCorrect = false;
			if (embersState.toLowerCase().equals(entry.state.toLowerCase()))
				embersStates++;
			else
				allCorrect = false;
			if (embersCity.toLowerCase().equals(entry.city.toLowerCase()))
				embersCities++;
			else
				allCorrect = false;

			if (allCorrect)
				embersCorrect++;

			totalResults++;

			// if PSL is correct and EMBERS missed, print list of locations
			System.out.print("Locations: ");
			JSONArray entitiesArray = json.getJSONObject("BasisEnrichment").getJSONArray("entities");

			for (int i = 0 ; i < entitiesArray.length(); i++) {
				Entity entity = new Entity(entitiesArray.getJSONObject(i));
				if (entity.getNeType().equals("LOCATION"))
					System.out.print(entity.getExpr() + ", ");
			}
			
		

			reader.close();
		}

		/* Prints results */
		double countryAccuracy = (double) correctCountries / totalResults;
		double stateAccuracy = (double) correctStates / totalResults;
		double cityAccuracy = (double) correctCities / totalResults;
		double totalAccuracy = (double) totalCorrect / totalResults;

		double embersCountryAccuracy = (double) embersCountries / totalResults;
		double embersStateAccuracy = (double) embersStates / totalResults;
		double embersCityAccuracy = (double) embersCities / totalResults;
		double embersTotalAccuracy = (double) embersCorrect / totalResults;

		out.println("\n\n\nPSL:");
		out.println("Country accuracy : " + countryAccuracy + "  (" + correctCountries + " / " + totalResults + ")");
		out.println("State accuracy : " + stateAccuracy + "  (" + correctStates + " / " + totalResults + ")");
		out.println("City accuracy : " + cityAccuracy + "  (" + correctCities + " / " + totalResults + ")");
		out.println("Total accuracy : " + totalAccuracy + "  (" + totalCorrect + " / " + totalResults + ")");

		out.println("\n\n\nEMBERS:");
		out.println("Country accuracy : " + embersCountryAccuracy + "  (" + embersCountries + " / " + totalResults + ")");
		out.println("State accuracy : " + embersStateAccuracy + "  (" + embersStates + " / " + totalResults + ")");
		out.println("City accuracy : " + embersCityAccuracy + "  (" + embersCities + " / " + totalResults + ")");
		out.println("Total accuracy : " + embersTotalAccuracy + "  (" + embersCorrect + " / " + totalResults + ")");
	}

	public static void main(String [] args) throws ConfigurationException, IOException, JSONException {
		ConfigBundle config = ConfigManager.getManager().getBundle("rss");

		String resultsPath = "./results"; //config.getString("enrichedpath", "");

		String gsrPath = config.getString("auxdatapath", "");
		String gsrFile = config.getString("gsr", "");
		String fullGSRPath = gsrPath + gsrFile;

		ResultsEvaluator eval = new ResultsEvaluator(resultsPath, fullGSRPath);
		eval.printEvalution(System.out);
	}

	private class gsrEntry {
		private int id;
		private String country;
		private String state;
		private String city;
		@SuppressWarnings("unused")
		private int code;
		@SuppressWarnings("unused")
		private String type;
		@SuppressWarnings("unused")
		private String population;
	}

}
