package iie.gaha.query;

import iie.gaha.common.QEConf;
import iie.gaha.common.RPoolProxy;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QE {
	public QEConf conf;
	public RPoolProxy rpp = new RPoolProxy(null);
	private ExecutorService pool;
	private static ConcurrentHashMap<Long, QJob> jobs;
	private ServerSocket ss;
	private Timer t;
	
	public QE() throws Exception {
		conf = new QEConf();
		__init();
	}
	
	public QE(QEConf conf) throws Exception {
		this.conf = conf;
		__init();
	}
	
	// response: SERVER_NAME
	public String getServerName() {
		return String.format("QE-%d-%s:%d", QEConf.serverId, 
				ss.getInetAddress().getHostAddress(), ss.getLocalPort());
	}
	
	public void __init() throws IOException {
//		pool = new ThreadPoolExecutor(10, 50, 1L, TimeUnit.MILLISECONDS,
//				new SynchronousQueue<Runnable>());
		pool = Executors.newCachedThreadPool();
		jobs = new ConcurrentHashMap<Long, QJob>();
		ss = new ServerSocket(conf.getServerPort());
	}
	
	public int init(String uri) throws Exception {
		t = new Timer("QETimer");
		int err = 0;

		rpp.init(uri);
		t.schedule(new QETimer(conf, 10), 1 * 1000, 10 * 1000);
		
		// shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdown QE server, release resources.");
				quit();
			}
		});

		return err;
	}
	
	public void startUp() {
		QExec qexec = new QExec(this);
		
		while (true) {
			try {
				pool.execute(new QEHandler(this, ss.accept()));
			} catch (Exception e) {
				e.printStackTrace();
				// BUG-XXX: do not shutdown the pool on any IOException.
				//pool.shutdown();
			}
		}
	}
	
	public void quit() {
		pool.shutdown();
		rpp.quit();
		if (t != null)
			t.cancel();
	}
	
	public static void keepJob(QJob job) {
		jobs.put(job.jobId, job);
	}
	
	public static void unkeepJob(QJob job) {
		jobs.remove(job.jobId);
	}
	
	public static int getRunningJobCount() {
		return jobs.size();
	}
}
