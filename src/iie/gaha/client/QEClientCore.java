package iie.gaha.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import iie.gaha.common.GenericFact;
import iie.gaha.common.QEClientConf;
import iie.gaha.common.RPoolProxy;
import iie.gaha.common.SocketHashEntry;

public class QEClientCore {
	private QEClientConf conf;
	
	public RPoolProxy rpp = new RPoolProxy(null);
	
	private AtomicInteger index = new AtomicInteger(0);				
	
	private List<String> keyList = Collections.synchronizedList(new ArrayList<String>());
	
	// 缓存与服务端的tcp连接,服务端名称到连接的映射
	private ConcurrentHashMap<String, SocketHashEntry> socketHash;
	
	private final Timer timer = new Timer("ActiveQEServerRefresher");
	
	private Map<Long, String> servers = new ConcurrentHashMap<Long, String>();

	public Map<Long, String> getServers() {
		return servers;
	}
	
	public QEClientCore() {
		conf = new QEClientConf();
	}
	
	private class QETimerTask extends TimerTask {
		@Override
		public void run() {
			try {
				refreshActiveQE(false);
			} catch (Exception e) {
				System.out.println("[ERROR] refresh active QE failed: " + 
						e.getMessage() + ".\n" + e.getCause());
			}
		}
	}
	
	public void init(String uri) throws Exception {
		rpp.init(uri);

		socketHash = new ConcurrentHashMap<String, SocketHashEntry>();
		// 从redis上获取所有的服务器地址
		refreshActiveQE(true);
		System.out.println("Got active server size=" + keyList.size());
		
		timer.schedule(new QETimerTask(), 500, conf.getServerRefreshInterval());
	}
	
	public void quit() {
		timer.cancel();
		rpp.quit();
	}
	
	private boolean getActiveQE() {
		Jedis jedis = RPoolProxy.rpL1.getResource();
		if (jedis == null) return false;
		
		try {
			Set<Tuple> active = jedis.zrangeWithScores("qe.active", 0, -1);
			Set<String> activeMMS = new TreeSet<String>();

			if (active != null && active.size() > 0) {
				for (Tuple t : active) {
					// translate ServerName to IP address
					String ipport = jedis.hget("mm.dns", t.getElement());

					// update server ID->Name map
					if (ipport == null) {
						servers.put((long)t.getScore(), t.getElement());
						ipport = t.getElement();
					} else
						servers.put((long)t.getScore(), ipport);

					String[] c = ipport.split(":");
					if (c.length == 2 && socketHash.get(ipport) == null) {
						Socket sock = new Socket();
						SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]), 
								conf.getSockPerServer());
						activeMMS.add(ipport);
						try {
							sock.setTcpNoDelay(true);
							sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
							she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
									new DataOutputStream(sock.getOutputStream()));
							if (socketHash.putIfAbsent(ipport, she) != null) {
								she.clear();
							}
						} catch (SocketException e) {
							System.out.println("[WARN] Connect to MMS " + c[0] + ":" + c[1] + 
									" failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						} catch (NumberFormatException e) {
							System.out.println("[FAIL] Transform string port(" + c[1] + 
									") to integer failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						} catch (IOException e) {
							System.out.println("[WARN] IO Error for MMS " + c[0] + ":" + c[1] +
									" failed: " + e.getMessage());
							e.printStackTrace();
							continue;
						}
					}
				}
			}
			synchronized (keyList) {
				keyList.addAll(activeMMS);
				keyList.retainAll(socketHash.keySet());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RPoolProxy.rpL1.putInstance(jedis);
		}
		
		
		return true;
	}
	
	private List<String> getActiveQEByHB() throws IOException {
		List<String> ls = new ArrayList<String>();
		Jedis jedis = RPoolProxy.rpL1.getResource();

		try {
			Set<String> keys = jedis.keys("qe.hb.*");

			for(String hp : keys) {
				String ipport = jedis.hget("qe.dns", hp.substring(6));
				if (ipport == null)
					ls.add(hp.substring(6));
				else
					ls.add(ipport);
			}
		} catch (JedisException e) {
			System.out.println("Get qe.hb.* failed: " + e.getMessage());
		} finally {
			RPoolProxy.rpL1.putInstance(jedis);
		}

		return ls;
	}
	
	private boolean refreshActiveQE(boolean isInit) {
		if (isInit) {
			return getActiveQE();
		} else {
			try {
				List<String> active = getActiveQEByHB();
				Set<String> activeQE = new TreeSet<String>();
				
				if (active.size() > 0) {
					for (String a : active) {
						String[] c = a.split(":");

						if (c.length == 2) {
							if (socketHash.get(a) == null) {
								// new MMS?
								Socket sock = new Socket();
								SocketHashEntry she = new SocketHashEntry(c[0], 
										Integer.parseInt(c[1]), 
										conf.getSockPerServer());
								try {
									sock.setTcpNoDelay(true);
									sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
									she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
											new DataOutputStream(sock.getOutputStream()));
									if (socketHash.putIfAbsent(a, she) != null) {
										she.clear();
									}
								} catch (SocketException e) {
									e.printStackTrace();
									continue;
								} catch (NumberFormatException e) {
									e.printStackTrace();
									continue;
								} catch (IOException e) {
									e.printStackTrace();
									continue;
								}
							}
							activeQE.add(a);
						}
					}
					synchronized (keyList) {
						keyList.clear();
						keyList.addAll(activeQE);
						keyList.retainAll(socketHash.keySet());
					}
				} else {
					keyList.clear();
				}
			} catch (IOException e) {
			}
		}
		return true;
	}
	
	private Set<String> __select_targets() throws Exception {
		if (keyList.size() == 0) {
			throw new Exception("No active QE server (" + keyList.size() + ").");
		}
		// roundrobin select 3 servers from keyList, if error in query, random
		// select in remain servers.
		int redundancy = Math.min(keyList.size(), conf.getRedundancy());
		Set<String> targets = new TreeSet<String>();
		int idx = index.getAndIncrement();
		if (idx < 0) {
			index.compareAndSet(idx, 0);
			idx = index.get();
		}
		for (int i = 0; i < redundancy; i++) {
			targets.add(keyList.get((idx + i) % keyList.size()));
		}
		
		return targets;
	}
	
	public List<GenericFact> qFact(long fid, double b, double e, long rid) throws Exception {
		return __qFact(__select_targets(), fid, b, e, rid);
	}

	private List<GenericFact> __qFact(Set<String> targets, long fid,
			double b, double e, long rid) throws Exception {
		Random rand = new Random();
		Set<String> saved = new TreeSet<String>();
		HashMap<String, Long> failed = new HashMap<String, Long>();
		List<GenericFact> r = null;
		
		do {
			for (String server : targets) {
				SocketHashEntry she = socketHash.get(server);
				if (she.probSelected()) {
					// BUG-XXX: we have to check if we can recover from this exception, 
					// then try our best to survive.
					try {
						r = QEClientExec.__qFact(she, fid, b, e, rid);
						saved.add(server);
					} catch (Exception e1) {
						if (failed.containsKey(server)) {
							failed.put(server, failed.get(server) + 1);
						} else
							failed.put(server, new Long(1));
						System.out.println("Query " + fid + " to " + server + 
								" failed: (" + e1.getMessage() + ") " +
										"for " + failed.get(server) + " times.");
					}
				} else {
					// this means target server has no current usable connection, we try to use 
					// another server
				}
			}
			if (saved.size() < 1) {
				List<String> remains = new ArrayList<String>(keyList);
				remains.removeAll(saved);
				for (Map.Entry<String, Long> e1 : failed.entrySet()) {
					if (e1.getValue() > 9) {
						remains.remove(e1.getKey());
					}
				}
				if (remains.size() == 0)
					break;
				targets.clear();
				for (int i = saved.size(); i < 1; i++) {
					targets.add(remains.get(rand.nextInt(remains.size())));
				}
			} else break;
		} while (true);
		
		if (saved.size() == 0) {
			throw new Exception("Error in Query: " + fid + 
					" [" + b + "," + e + "] " + rid);
		}
		return r;
	}
}
