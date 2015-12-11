package iie.gaha.common;

public class GenericFact {
	public long fact_id;
	public long attr;
	public long[] facts;
	
	public GenericFact() {
	}
	
	public GenericFact(long fact_id, long attr, long[] facts) {
		this.fact_id = fact_id;
		this.attr = attr;
		this.facts = facts;
	}
	
	public long getLength() {
		if (facts != null)
			return 8 + 8 + 8 * facts.length;
		else
			return 8 + 8;
	}
}
