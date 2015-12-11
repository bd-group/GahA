package iie.gaha.query;

import iie.gaha.common.QEConf;
import iie.gaha.common.RPoolProxy;

import java.util.TimerTask;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

public class QETimer extends TimerTask {
	
	private QEConf conf;
	private int period;
	private String hbkey;

	public QETimer(QEConf conf, int period) throws JedisException {
		super();
		this.period = period;
		this.conf = conf;
		
		// FIXME: register to L1 pool, and get QE id
		// FIXME: add heartbeat info to L1 pool
		Jedis jedis = RPoolProxy.rpL1.getResource();
		if (jedis == null)
			throw new JedisException("Get default jedis instance failed.");
		try {
			hbkey = "qe.hb." + conf.getNodeName() + ":" + conf.getServerPort();
			Pipeline pi = jedis.pipelined();
			pi.set(hbkey, "1");
			pi.expire(hbkey, period + 5);
			pi.sync();
			
			// update qe.dns for IP info
			if (conf.getOutsideIP() != null) {
				jedis.hset("qe.dns", conf.getNodeName() + ":" + conf.getServerPort(), 
						conf.getOutsideIP() + ":" + conf.getServerPort());
				System.out.println("Update qe.dns for " + conf.getNodeName() + " -> " + 
						conf.getOutsideIP());
			}
			
			// determine the ID of ourself, register ourself
			String self = conf.getNodeName() + ":" + conf.getServerPort();
			Long sid;
			if (jedis.zrank("qe.active", self) == null) {
				sid = jedis.incr("qe.next.serverid");
				// FIXME: if two server start with the same port, fail!
				jedis.zadd("qe.active", sid, self);
			}
			
			// reget the sid
			sid = jedis.zscore("qe.active", self).longValue();
			QEConf.serverId = sid;
			System.out.println("Got ServerID " + sid + " for Server " + self);
		} finally {
			RPoolProxy.rpL1.putInstance(jedis);
		}
	}
	
	@Override
	public void run() {
		try {
			Jedis jedis = RPoolProxy.rpL1.getResource();
			try {
				if (jedis != null) {
					Pipeline pi = jedis.pipelined();
					pi.set(hbkey, "1");
					pi.expire(hbkey, period + 5);
					pi.sync();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				RPoolProxy.rpL1.putInstance(jedis);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
