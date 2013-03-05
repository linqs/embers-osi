package edu.umd.cs.linqs.embers;

import java.text.Normalizer;

public class NormalizeText {

	public static String stripAccents(String s) {
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		s = s.replaceAll("[^\\p{ASCII}]", "");
		return s;
	}
}
