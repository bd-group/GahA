package iie.gaha.common;

import java.util.Set;

import redis.clients.jedis.HostAndPort;

public class GahAConf {
	public enum RedisMode {
		STANDALONE, SENTINEL, CLUSTER,
	};

	// for standalone mode
	private HostAndPort hap;
	
	// for sentinel mode
	private Set<String> sentinels;
	
	// redis mode: sentinel or standalone
	private RedisMode redisMode;

	// redis connect timeout in 30s
	private int redisTimeout = 30 * 1000;
	
	// should RPS use cache?
	private boolean rpsUseCache = false;
	
	public HostAndPort getHap() {
		return hap;
	}

	public void setHap(HostAndPort hap) {
		this.hap = hap;
	}

	public Set<String> getSentinels() {
		return sentinels;
	}

	public void setSentinels(Set<String> sentinels) {
		this.sentinels = sentinels;
	}

	public RedisMode getRedisMode() {
		return redisMode;
	}

	public void setRedisMode(RedisMode redisMode) {
		this.redisMode = redisMode;
	}

	public int getRedisTimeout() {
		return redisTimeout;
	}

	public void setRedisTimeout(int redisTimeout) {
		this.redisTimeout = redisTimeout;
	}

	public boolean isRpsUseCache() {
		return rpsUseCache;
	}

	public void setRpsUseCache(boolean rpsUseCache) {
		this.rpsUseCache = rpsUseCache;
	}
}
