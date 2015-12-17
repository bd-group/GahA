package iie.gaha.common;

import java.util.concurrent.ConcurrentHashMap;

public class Graph {
	private ConcurrentHashMap<Long, GenericFact> g;
	
	public Graph() {
		this.setG(new ConcurrentHashMap<Long, GenericFact>());
	}

	public ConcurrentHashMap<Long, GenericFact> getG() {
		return g;
	}

	public void setG(ConcurrentHashMap<Long, GenericFact> g) {
		this.g = g;
	}
}
