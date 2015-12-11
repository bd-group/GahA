package iie.gaha.common;

public class QEClientConf {
	private GahAConf gconf;
	private int sockPerServer = 5;
	private int redundancy = 3;
	private int serverRefreshInterval = 5 * 1000;
	
	public QEClientConf() {
		this.setGconf(new GahAConf());
	}
	
	public QEClientConf(GahAConf gconf) {
		this.setGconf(gconf);
	}

	public GahAConf getGconf() {
		return gconf;
	}

	public void setGconf(GahAConf gconf) {
		this.gconf = gconf;
	}

	public int getSockPerServer() {
		return sockPerServer;
	}

	public void setSockPerServer(int sockPerServer) {
		this.sockPerServer = sockPerServer;
	}

	public int getRedundancy() {
		return redundancy;
	}

	public void setRedundancy(int redundancy) {
		this.redundancy = redundancy;
	}

	public int getServerRefreshInterval() {
		return serverRefreshInterval;
	}

	public void setServerRefreshInterval(int serverRefreshInterval) {
		this.serverRefreshInterval = serverRefreshInterval;
	}
}
