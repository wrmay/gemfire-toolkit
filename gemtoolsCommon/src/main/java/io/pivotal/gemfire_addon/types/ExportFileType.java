package io.pivotal.gemfire_addon.types;

public enum ExportFileType {
	ADP_DEFAULT_FORMAT ("adp")
	;
	
	private final String s;
	
	ExportFileType(String s) {
		this.s = s;
	}
	
	public String toString() {
		return this.s;
	}
}
