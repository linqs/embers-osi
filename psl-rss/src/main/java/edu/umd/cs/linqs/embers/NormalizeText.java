package edu.umd.cs.linqs.embers;

import java.text.Normalizer;

public class NormalizeText {

	public static String stripAccents(String s) {
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		s = s.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		return s;
	}
	
	public static void main(String[] args) {
		String s = "Bol�var";
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		System.out.println(s);
		s = s.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		System.out.print(s);
	}
}
