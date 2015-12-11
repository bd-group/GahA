package iie.gaha.common;

import java.util.List;

public class Graph {
	private List<Long> nodes;
	private List<Object> nodesAttr;
	private List<Long> efrom;
	private List<Long> eto;

	public Graph(List<Long> nodes, List<Long> efrom, List<Long> eto) {
		this.nodes = nodes;
		this.efrom = efrom;
		this.eto = eto;
	}
	
	public List<Long> getNodes() {
		return nodes;
	}
	
	public List<Object> getNodesAttr() {
		return nodesAttr;
	}
	
	public List<Long> getEfrom() {
		return efrom;
	}
	
	public List<Long> getEto() {
		return eto;
	}
	
	public long getNodeCount() {
		return nodes.size();
	}
	
	public long getEdgeCount() {
		return efrom.size();
	}
	
	public Edge getEdge(int idx) {
		if (idx >= 0 && idx < efrom.size())
			return new Edge(efrom.get(idx), eto.get(idx));
		else
			return null;
	}
}
