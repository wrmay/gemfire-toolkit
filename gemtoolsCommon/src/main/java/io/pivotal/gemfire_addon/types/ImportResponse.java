package io.pivotal.gemfire_addon.types;

import java.io.File;
import java.io.Serializable;

public class ImportResponse implements Comparable<ImportResponse>, Serializable {
	private static final long serialVersionUID = 1L;

	private transient File file;
	private String  fileDir;
	private String	fileName;
	private String  memberName;
	private String  hostName;
	private int     recordsRead;
	private int		recordsWritten;

	// Sort on host name then member name. Should be null or present in this and that instances.
	public int compareTo(ImportResponse that) {
		String[] thisFields = new String[] { this.getHostName(), this.getMemberName(), this.getFileName() };
		String[] thatFields = new String[] { that.getHostName(), that.getMemberName(), that.getFileName() };
		
		for(int i=0 ; i< thisFields.length ; i++) {
			if(thisFields[i]!=null && thatFields[i]!=null && !(thisFields[i].equalsIgnoreCase(thatFields[i]))) {
				return thisFields[i].compareToIgnoreCase(thatFields[i]);
			}
		}
		
		return 0;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public String getFileDir() {
		return fileDir;
	}

	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getMemberName() {
		return memberName;
	}

	public void setMemberName(String memberName) {
		this.memberName = memberName;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getRecordsRead() {
		return recordsRead;
	}

	public void setRecordsRead(int recordsRead) {
		this.recordsRead = recordsRead;
	}

	public int getRecordsWritten() {
		return recordsWritten;
	}

	public void setRecordsWritten(int recordsWritten) {
		this.recordsWritten = recordsWritten;
	}

	@Override
	public String toString() {
		return "ImportResponse [fileDir=" + fileDir + ", fileName=" + fileName
				+ ", memberName=" + memberName + ", hostName=" + hostName
				+ ", recordsRead=" + recordsRead + ", recordsWritten="
				+ recordsWritten + "]";
	}


}
