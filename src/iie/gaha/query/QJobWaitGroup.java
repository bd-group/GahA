package iie.gaha.query;

import iie.gaha.query.QJob.QStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QJobWaitGroup {
	public ConcurrentHashMap<Long, QJob> group = new ConcurrentHashMap<Long, QJob>();
	
	public QJobWaitGroup() {
	}
	
	public void addToGroup(QJob job) {
		group.put(job.jobId, job);
		job.setGroup(this);
	}
	
	public void delFromGroup(QJob job) {
		group.remove(job.jobId);
	}
	
	public long getSize() {
		return group.size();
	}
	
	public QJob getAnyJob() {
		QJob job = null;
		
		do {
			for (Map.Entry<Long, QJob> e : group.entrySet()) {
				if (e.getValue().status != QStatus.RUNNING) {
					// ok, this job has been done
					job = e.getValue();
					break;
				}
			}
			if (job != null) {
				group.remove(job.jobId);
				break;
			}
			if (group.size() == 0) break;

			synchronized (group) {
				try {
					group.wait(1);
				} catch (InterruptedException e1) {
				}
			}
		} while (true);

		return job;
	}
}
