package iie.gaha.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SocketHashEntry {
	public String hostname;
	public int port, cnr;
	AtomicInteger xnr = new AtomicInteger(0);
	public Map<Long, SEntry> map;
	AtomicLong nextId = new AtomicLong(0);

	public static class SEntry {
		public Socket sock;
		public long id;
		public boolean used;
		public DataInputStream dis;
		public DataOutputStream dos;

		public SEntry(Socket sock, long id, boolean used, DataInputStream dis, 
				DataOutputStream dos) {
			this.sock = sock;
			this.id = id;
			this.used = used;
			this.dis = dis;
			this.dos = dos;
		}
	}

	public SocketHashEntry(String hostname, int port, int cnr) {
		this.hostname = hostname;
		this.port = port;
		this.cnr = cnr;
		this.map = new ConcurrentHashMap<Long, SEntry>();
	}

	public void setFreeSocket(long id) {
		SEntry e = map.get(id);
		if (e != null) {
			e.used = false;
		}
		synchronized (this) {
			this.notify();
		}
	}

	public boolean probSelected() {
		if (map.size() > 0)
			return true;
		else {
			// 1/100 prob selected
			if (new Random().nextInt(100) == 0)
				return true;
			else 
				return false;
		}
	}

	public long getFreeSocket() throws IOException {
		boolean found = false;
		long id = -1;

		do {
			synchronized (this) {
				for (SEntry e : map.values()) {
					if (!e.used) {
						// ok, it is unused
						found = true;
						e.used = true;
						id = e.id;
						break;
					}
				}
			}

			if (!found) {
				if (map.size() + xnr.get() < cnr) {
					// do connect now
					Socket socket = new Socket();
					xnr.getAndIncrement();
					try {
						socket.connect(new InetSocketAddress(this.hostname, this.port));
						socket.setTcpNoDelay(true);
						id = this.addToSocketsAsUsed(socket, 
								new DataInputStream(socket.getInputStream()), 
								new DataOutputStream(socket.getOutputStream()));
						//new DataInputStream(new BufferedInputStream(socket.getInputStream())), 
						//new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
						System.out.println("[GFS] New connection @ " + id + " for " + 
								hostname + ":" + port);
					} catch (SocketException e) {
						xnr.getAndDecrement();
						System.out.println("[GFS] Connect to " + hostname + ":" + port + 
								" failed w/ " + e.getMessage());
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
						}
						throw e;
					} catch (Exception e) {
						xnr.getAndDecrement();
						System.out.println("[GFS] Connect to " + hostname + ":" + port + 
								" failed w/ " + e.getMessage());
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
						}
						throw new IOException(e.getMessage());
					}
					xnr.getAndDecrement();
				} else {
					do {
						try {
							synchronized (this) {
								//System.out.println("wait ...");
								this.wait(60000);
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
							continue;
						}
						break;
					} while (true);
				}
			} else {
				break;
			}
		} while (id == -1);

		return id;
	}

	public long addToSocketsAsUsed(Socket sock, DataInputStream dis, 
			DataOutputStream dos) {
		SEntry e = new SEntry(sock, nextId.getAndIncrement(), true, dis, dos);
		synchronized (this) {
			map.put(e.id, e);
		}
		return e.id;
	}

	public void addToSockets(Socket sock, DataInputStream dis, 
			DataOutputStream dos) {
		SEntry e = new SEntry(sock, nextId.getAndIncrement(), false, dis, dos);
		synchronized (this) {
			map.put(e.id, e);
		}
	}

	public void useSocket(long id) {
		synchronized (this) {
			SEntry e = map.get(id);
			if (e != null) {
				e.used = true;
			}
		}
	}

	public void delFromSockets(long id) {
		System.out.println("Del sock @ " + id + " for " + hostname + ":" + port);
		SEntry e = null;
		synchronized (this) {
			e = map.get(id);
			map.remove(id);
			this.notifyAll();
		}
		if (e != null) {
			try {
				e.dis.close();
				e.dos.close();
				e.sock.close();
			} catch (IOException e1) {
			}
		}
	}

	public void clear() {
		synchronized (this) {
			for (Map.Entry<Long, SEntry> e : map.entrySet()) {
				try {
					e.getValue().sock.close();
				} catch (IOException e1) {
				}
			}
		}
	}
}
