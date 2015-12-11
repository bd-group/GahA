package iie.gaha.common;

import java.net.InetAddress;

public class QEConf {
	
	private GahAConf gconf;
	
	private String nodeName;
	
	private String outsideIP;
	
	// QE server id, should register to L1 pool
	public static long serverId;
	
	public enum QEExecMode {
		STANDALONE, // each QE server stand alone
		CLUSTER,	// QE server num must equal as redis server num
	}
	
	private String lmdb_prefix = "./data";
	
	private int query_queue_max_length = 1000;
	
	private int serverPort = 23233;
	
	private static int recv_buffer_size = 256 * 1024;
	
	private static int send_buffer_size = 5 * 1024 * 1024;
	
	public QEConf() throws Exception {
		gconf = new GahAConf();
		nodeName = InetAddress.getLocalHost().getHostName();
	}
	
	public QEConf(GahAConf gconf, String lmdb_prefix) throws Exception {
		this.setGconf(gconf);
		this.setLmdb_prefix(lmdb_prefix);
		nodeName = InetAddress.getLocalHost().getHostName();
	}

	public GahAConf getGconf() {
		return gconf;
	}

	public void setGconf(GahAConf gconf) {
		this.gconf = gconf;
	}

	public String getLmdb_prefix() {
		return lmdb_prefix;
	}

	public void setLmdb_prefix(String lmdb_prefix) {
		this.lmdb_prefix = lmdb_prefix;
	}

	public int getQuery_queue_max_length() {
		return query_queue_max_length;
	}

	public void setQuery_queue_max_length(int query_queue_max_length) {
		this.query_queue_max_length = query_queue_max_length;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public static int getRecv_buffer_size() {
		return recv_buffer_size;
	}

	public static int getSend_buffer_size() {
		return send_buffer_size;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getOutsideIP() {
		return outsideIP;
	}

	public void setOutsideIP(String outsideIP) {
		this.outsideIP = outsideIP;
	}

}
