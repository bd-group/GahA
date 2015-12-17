package iie.gaha.query;

import java.util.concurrent.atomic.AtomicLong;

public class QJob {
	private static AtomicLong nextJobId = new AtomicLong(0);
	private static long minJobId = -1;
	
	public enum QOp {
		QFACT,
		QFACT_GRAPH,
	}
	
	public enum QStatus {
		RUNNING,
		DONE,
		ERR,
	}
	
	public static class QJobArgs {
		// fact id
		long id;
		// time range
		double b, e;
		// relation id
		long rid;
		
		public QJobArgs(long id, double b, double e, long rid) {
			this.id = id;
			this.b = b;
			this.e = e;
			this.rid = rid; 
		}
	}
	
	public long jobId;
	public QOp qop;
	public QJobArgs args;
	
	public Object r;
	public QStatus status = QStatus.RUNNING;
	
	private QJobWaitGroup group;
	
	public long beginTs = -1, endTs = -1;
	
	public QJob(QOp qop, QJobArgs args) {
		this.qop = qop;
		this.args = args;
		jobId = getNextJobId();
	}
	
	public long getLatency() {
		if (beginTs != -1 && endTs != -1)
			return endTs - beginTs;
		else
			return -1;
	}
	
	private long getNextJobId() {
		return nextJobId.getAndIncrement();
	}
	
	public static long getCurrentJobId() {
		return nextJobId.get();
	}

	public static long getMinJobId() {
		return minJobId;
	}

	public static void setMinJobId(long minJobId) {
		QJob.minJobId = minJobId;
	}

	public QJobWaitGroup getGroup() {
		return group;
	}

	public void setGroup(QJobWaitGroup group) {
		this.group = group;
	}
	
	public String toString() {
		return "JOB " + jobId + " " + qop +
				" FID=" + args.id +
				" b=" + args.b + 
				" e=" + args.e + 
				" rid= " + args.id +
				" exec " + status;
	}
}
