package io.pivotal.gemfire_addon.types;

public enum AdpExportRecordType {
	HEADER 			((byte)0)
	,HINT_KEY 		((byte)1)
	,HINT_VALUE 	((byte)2)
	,DATA 			((byte)4)
	,FOOTER 		((byte)8)
	;
	
	private final byte b;
	
	AdpExportRecordType(Byte b) {
		this.b = b;
	}
	
	public byte getB() {
		return this.b;
	}
	
	public String toString() {
		return this.b + "";
	}
}
