package edu.umd.cs.linqs.embers.json;

import org.json.JSONException;
import org.json.JSONObject;

public class Entity {

	public String getExpr() {
		return expr;
	}
	public void setExpr(String expr) {
		this.expr = expr;
	}
	public String getNeType() {
		return neType;
	}
	public void setNeType(String neType) {
		this.neType = neType;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Entity(JSONObject entity) throws JSONException {
		expr = entity.getString("expr");
		neType = entity.getString("neType");
		
		String offsetStr = entity.getString("offset");
		offset = Integer.parseInt(offsetStr.substring(0, offsetStr.indexOf(':')));		
	}
	
	private String expr;
	private String neType;
	private int offset;
	
}
