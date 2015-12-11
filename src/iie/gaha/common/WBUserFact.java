package iie.gaha.common;

public class WBUserFact {
	// relation id
	public long r;
	// Fact_IDs array
	public long[] a;
	// user fans number
	public long f;
	
	public WBUserFact() {
	}
	
	public WBUserFact(String rid, String[] fids, long fans) {
		this.r = Long.parseLong(rid);
		if (fids != null && fids.length > 0) {
			this.a = new long[fids.length];
			for (int i = 0, j = 0; i < fids.length; i++) {
				try {
					Long n = Long.parseLong(fids[i]);
					this.a[j] = n;
					j++;
				} catch (Exception e) {
				}
			}
		}
		this.f = fans;
	}
}
