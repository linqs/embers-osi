package edu.umd.cs.linqs.embers;

import org.json.JSONObject;

public class DummyProcessor implements JSONProcessor {
	
	@Override
	public JSONObject process(JSONObject json) {
		return json;
	}

}
