package edu.umd.cs.linqs.embers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class JointPredictionTester {

	public static void main(String [] args) {
		try {
			Scanner scanner = new Scanner(new File("/Users/bert/Dropbox/Research/OSI/jointEvent/test.json"));
			FileWriter fw;
			fw = new FileWriter(new File("testOut.json"));
			JSONProcessor rewriter = new PSLJointRewriter();

			while (scanner.hasNext()) {
				String line = scanner.nextLine();

				String result = rewriter.process(line);

				fw.write(result + "\n");

			}
			
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
