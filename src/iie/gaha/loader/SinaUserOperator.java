package iie.gaha.loader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.xerial.snappy.Snappy;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import iie.gaha.common.RPoolProxy;
import iie.gaha.common.RedisPoolSelector.RedisConnection;
import iie.gaha.common.WBUser;
import iie.gaha.common.WBUserFact;

public class SinaUserOperator {
	public RPoolProxy rpp;
	
	public enum CALGO {
		GZIP, SNAPPY,
	}
	
	private int verbose = 0;
	private CALGO calgo = CALGO.SNAPPY;
	private int MAX_BYTES_TO_FLUSH = 256 * 1024;
	private AtomicLong sendBytes = new AtomicLong(0);
	private AtomicLong redisCall = new AtomicLong(0);
	private ConcurrentHashMap<String, __WBUFactQ> sMap = 
			new ConcurrentHashMap<String, __WBUFactQ>();

	public SinaUserOperator(RPoolProxy rpp) {
		this.rpp = rpp;
	}
	
	private class __WBUFact {
		public String fid;
		public Double time;
		public byte[] data;
		
		public __WBUFact(String fid, Double time, byte[] data) {
			this.fid = fid;
			this.time = time;
			this.data = data;
		}
	}
	
	private class __WBUFactQ {
		public ArrayList<__WBUFact> facts;
		public long length;
		
		public __WBUFactQ() {
			facts = new ArrayList<__WBUFact>();
		}
		
		public void addFact(String fid, Double time, byte[] data) {
			synchronized (this) {
				facts.add(new __WBUFact(fid, time, data));
				length += fid.length() + data.length;
			}
		}
		
		public void clear() {
			synchronized (this) {
				facts.clear();
				length = 0;
			}
		}
	}
	
	// FACT: (user, time, relation)
	public boolean addFact(String uid, Double time, WBUserFact fact) {
		RedisConnection rc = null;
		String raw = null, fid = "F" + uid;
		
		raw = JSON.toJSONString(fact);
		byte[] compressed = null;
		
		switch (calgo) {
		case SNAPPY:
			try {
				compressed = Snappy.compress(raw);
			} catch (IOException e1) {
				e1.printStackTrace();
				return false;
			}
			break;
		case GZIP:
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try {
				GZIPOutputStream gzip = new GZIPOutputStream(out);
				gzip.write(raw.getBytes());
				gzip.close();
				out.close();
			} catch (IOException e1) {
				e1.printStackTrace();
				return false;
			}
			compressed = out.toByteArray();
		}

		if (getVerbose() >= 3)
			System.out.println("RAW " + raw.length() + 
					" vs SNAPPY " + compressed.length);
		try {
			rc = RPoolProxy.rps.getL2("F" + uid, true);
			if (rc.jedis != null) {
				__WBUFactQ q = sMap.get(rc.id);
				if (q == null) {
					q = new __WBUFactQ();
					__WBUFactQ tmpQ = sMap.putIfAbsent(rc.id, q);
					if (tmpQ != null) q = tmpQ;
				}
				q.addFact(fid, time, compressed);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RPoolProxy.rps.putL2(rc);
		}
		
		return flushFact(false);
	}
	
	private boolean flushFact(boolean force) {
		for (Map.Entry<String, __WBUFactQ> e : sMap.entrySet()) {
			if (e.getValue().length >= MAX_BYTES_TO_FLUSH || force) {
				RedisConnection rc = null;
				
				try {
					rc = RPoolProxy.rps.getL2ByPid(e.getKey());
					Jedis jedis = rc.jedis;
					if (jedis != null) {
						Pipeline p = jedis.pipelined();
						for (__WBUFact f : e.getValue().facts) {
							p.zadd(f.fid.getBytes(), f.time, f.data);
						}
						p.sync();
						redisCall.incrementAndGet();
						sendBytes.addAndGet(e.getValue().length);
						e.getValue().clear();
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					RPoolProxy.rps.putL2(rc);
				}
			}
		}
		return true;
	}
	
	public void flush() {
		flushFact(true);
	}

	public boolean addFactSlow(String uid, Double time, WBUserFact fact) {
		RedisConnection rc = null;
		String raw = null, fid = "F" + uid;
		long r = 0;
		
		raw = JSON.toJSONString(fact);
		byte[] compressed = null;
		try {
			compressed = Snappy.compress(raw);
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		if (getVerbose() >= 3)
			System.out.println("RAW " + raw.length() + 
					" vs SNAPPY " + compressed.length);
		try {
			rc = RPoolProxy.rps.getL2("F" + uid, true);
			Jedis jedis = rc.jedis;
			if (jedis != null) {
				r = jedis.zadd(fid.getBytes(), time, compressed);
				redisCall.incrementAndGet();
				sendBytes.addAndGet(fid.length() + compressed.length);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RPoolProxy.rps.putL2(rc);
		}
		
		if (r > 0)
			return true;
		else
			return false;
	}

	// Relation: (json object + snappy zipped)
	public boolean addRelation(WBUser user) {
		RedisConnection rc = null;
		String raw = null;
		
		raw = JSON.toJSONString(user);
		byte[] compressed = null;
		try {
			compressed = Snappy.compress(raw);
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}

		try {
			rc = RPoolProxy.rps.getL2("R" + user._id, true);
			Jedis jedis = rc.jedis;
			if (jedis != null)
				jedis.set(user._id.getBytes(), compressed);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RPoolProxy.rps.putL2(rc);
		}
		
		return true;
	}

	public int getVerbose() {
		return verbose;
	}

	public void setVerbose(int verbose) {
		this.verbose = verbose;
	}

	public long getSendBytes() {
		return sendBytes.get();
	}
	
	public long getRedisCall() {
		return redisCall.get();
	}
}
