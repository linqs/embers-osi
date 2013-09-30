package edu.umd.cs.linqs.embers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;

public class JointPredictionTester {

	public static void main(String [] args) {
		if (args.length < 2) 
			throw new IllegalArgumentException("Usage: JointPredictionTester <input_json_file> <output_json_file> <optional_model>");

		int total = 0;
		int originalTypeCorrect = 0;
		int originalViolentCorrect = 0;
		int originalPopulationCorrect = 0;
		int pslTypeCorrect = 0;
		int pslViolentCorrect = 0;
		int pslPopulationCorrect = 0;
		
		int suppressCount = 0;
		int emitCount = 0;
		double suppressQuality = 0;
		double emitQuality = 0;

		try {
			Scanner scanner = new Scanner(new File(args[0]));
			FileWriter fw;
			fw = new FileWriter(new File(args[1]));
			PSLJointRewriter rewriter = (args.length < 3)? new PSLJointRewriter(): new PSLJointRewriter(args[2]);

			while (scanner.hasNext()) {
				String line = scanner.nextLine();

				String result = rewriter.process(line);

				fw.write(result + "\n");

				try {	
					JSONObject originalWarning = new JSONObject(line);

					if (originalWarning.has("matched_gsr")) {
						JSONObject pslWarning = new JSONObject(result);
						JSONObject gsr = originalWarning.getJSONObject("matched_gsr");

						String eventType = gsr.getString("eventType");
						String type = eventType.substring(0, 3);
						String violent = eventType.substring(4, 4);
						String population = gsr.getString("population");

						String originalEventType = originalWarning.getString("eventType");
						String originalType = originalEventType.substring(0, 3);
						String originalViolent = originalEventType.substring(4, 4);
						String originalPopulation = originalWarning.getString("population");

						String pslEventType = pslWarning.getString("eventType");
						String pslType = pslEventType.substring(0, 3);
						String pslViolent = pslEventType.substring(4, 4);
						String pslPopulation = pslWarning.getString("population");
						
						total++;
						if (type.equals(originalType)) 
							originalTypeCorrect++;
						if (violent.equals(originalViolent)) 
							originalViolentCorrect++;
						if (population.equals(originalPopulation)) 
							originalPopulationCorrect++;
						if (type.equals(pslType)) 
							pslTypeCorrect++;
						if (violent.equals(pslViolent)) 
							pslViolentCorrect++;
						if (population.equals(pslPopulation)) 
							pslPopulationCorrect++;
						
						if (pslWarning.getBoolean("suppress")) {
							suppressCount++;
							suppressQuality += originalWarning.getJSONObject("match_score").getJSONObject("mean_score").getDouble("total_quality");
						} else {
							emitCount++;
							emitQuality += originalWarning.getJSONObject("match_score").getJSONObject("mean_score").getDouble("total_quality");
						}
					}

				} catch (JSONException e) {
					e.printStackTrace();
				}

			}

			if (total > 0) {
				System.out.println("Original");
				System.out.println("Type accuracy: " + (double) originalTypeCorrect / (double) total + " (" + originalTypeCorrect + " / " + total + ")");
				System.out.println("Violent accuracy: " + (double) originalViolentCorrect / (double) total + " (" + originalViolentCorrect + " / " + total + ")");
				System.out.println("Population accuracy: " + (double) originalPopulationCorrect / (double) total + " (" + originalPopulationCorrect + " / " + total + ")");
				System.out.println("\nPSL");
				System.out.println("Type accuracy: " + (double) pslTypeCorrect / (double) total + " (" + pslTypeCorrect + " / " + total + ")");
				System.out.println("Violent accuracy: " + (double) pslViolentCorrect / (double) total + " (" + pslViolentCorrect + " / " + total + ")");
				System.out.println("Population accuracy: " + (double) pslPopulationCorrect / (double) total + " (" + pslPopulationCorrect + " / " + total + ")");
				System.out.println("\nSuppression Stats:");
				System.out.println("Total emitted: " + emitCount + ", average original quality: " + emitQuality / (double) emitCount);
				System.out.println("Total suppressed: " + suppressCount + ", average original quality: " + suppressQuality / (double) suppressCount);
			}

			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

}
