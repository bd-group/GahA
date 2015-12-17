package iie.gaha.query;

import iie.gaha.common.GenericFact;
import iie.gaha.common.Graph;
import iie.gaha.common.RPoolProxy;
import iie.gaha.common.RedisPoolSelector.RedisConnection;
import iie.gaha.common.WBUserFact;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.xerial.snappy.Snappy;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.Jedis;

public class QExec {
	private QE qe;
	private static ExecutorService pool = Executors.newCachedThreadPool(); 
	private static LinkedBlockingQueue<QJob> queue = new LinkedBlockingQueue<QJob>(1000);

	public QExec(QE qe) {
		this.qe = qe;
		new Thread(new JobChecker()).start();
	}
	
	public static void execJob(QJob job) {
		do {
			try {
				queue.put(job);
				break;
			} catch (InterruptedException e) {
				continue;
			}
		} while (true);
		QE.keepJob(job);
	}
	
	private class JobChecker implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					QJob job = queue.take();
					if (job != null) {
						pool.execute(new QExecutor(job));
					}
				} catch (InterruptedException e) {
				}
			}
		}
	}
	
	private class QExecutor implements Runnable {
		public QJob job;
		
		public QExecutor(QJob job) {
			this.job = job;
		}
		
		@Override
		public void run() {
			try {
				job.beginTs = System.currentTimeMillis();
				switch (job.qop) {
				case QFACT:
					job.r = QExec.qFact(job.args.id, 
							job.args.b, job.args.e, job.args.rid);
					break;
				case QFACT_GRAPH:
					job.r = QExec.qFactGraph(job.args.id, 
							job.args.b, job.args.e, job.args.rid);
					break;
				}
				job.status = QJob.QStatus.DONE;
			} catch (Exception e) {
				e.printStackTrace();
				job.r = e.getMessage();
				job.status = QJob.QStatus.ERR;
			}
			job.endTs = System.currentTimeMillis();

			synchronized (job.getGroup().group) {
				job.getGroup().group.notifyAll();
			}
		}
	}

	// Query for Fact:
	// id + [time range] + relation -> list<fid>
	public static List<GenericFact> qFact(long id, Double b, Double e, long rid) {
		RedisConnection rc = null;
		String fid = "F" + id;
		ArrayList<GenericFact> facts = new ArrayList<GenericFact>();
		
		try {
			long bTs = System.nanoTime();
			rc = RPoolProxy.rps.getL2(fid, false);
			System.out.println("getL2 " + ((System.nanoTime() - bTs) / 1000));
			Jedis jedis = rc.jedis;
			if (jedis != null) {
				bTs = System.nanoTime();
				Set<byte[]> r = jedis.zrangeByScore(fid.getBytes(), b, e);
				System.out.println("zRange " + ((System.nanoTime() - bTs) / 1000));
				bTs = System.nanoTime();
				for (byte[] bf : r) {
					byte[] raw = Snappy.uncompress(bf);
					WBUserFact f = JSON.parseObject(raw, WBUserFact.class);
					if (f.r == rid || rid == -1) {
						facts.add(new GenericFact(id, f.f, f.a));
					}
				}
				System.out.println("uncomp " + ((System.nanoTime() - bTs) / 1000));
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			RPoolProxy.rps.putL2(rc);
		}
		
		return facts;
	}
	
	// Query for Fact SubGraph:
	// id + [time range] + relation -> list<fid>
	public static List<GenericFact> qFactGraph(long id, Double b, Double e, long rid) {
		// SLOW IMPLEMENTATION
		return qFact(id, b, e, rid);
	}
}
