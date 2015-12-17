package iie.gaha.query;

import iie.gaha.common.GenericFact;
import iie.gaha.common.QEConf;
import iie.gaha.common.QueryFlag;
import iie.gaha.common.QueryType;
import iie.gaha.query.QJob.QJobArgs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QEHandler implements Runnable {
	private QE qe;
	private Socket s;
	
	private DataInputStream dis;
	private DataOutputStream dos;
	
	public QEHandler(QE qe, Socket s) throws Exception {
		this.qe = qe;
		this.s = s;
		s.setTcpNoDelay(true);
		dis = new DataInputStream(this.s.getInputStream());
		dos = new DataOutputStream(this.s.getOutputStream());
	}
	
	private static final ThreadLocal<byte[]> threadLocalRecvBuffer = 
			new ThreadLocal<byte[]>() {
				@Override
				protected synchronized byte[] initialValue() {
					return new byte[QEConf.getRecv_buffer_size()];
				}
			};

	private static final ThreadLocal<byte[]> threadLocalSendBuffer = 
			new ThreadLocal<byte[]>() {
				@Override
				protected synchronized byte[] initialValue() {
					return new byte[QEConf.getSend_buffer_size()];
				}
			};

	@Override
	public void run() {
		try {
			while (true) {
				// header[0] -> QueryType
				// header[1] -> QueryFlag
				byte[] header = new byte[4];
				
				dis.readFully(header);
				switch (header[0]) {
				case QueryType.HELLO:
					// request : HEADER | LEN | CLIENT_NAME
					// response: HEADER | LEN | SERVER_NAME
					String s = qe.getServerName();
					dos.write(header);
					dos.write(s.length());
					dos.write(s.getBytes());
					break;
				case QueryType.QFACT:
				{
					// Query FACTs by fact_id (1-layer)
					//
					// request : HEADER | LEN | NR | [{FACT_ID(L),b(D),e(D),R_ID(L)]
					// response: HEADER | LEN | NR | [{FACT_ID,ATTR,[FACT_ID]}](ziped)
					int len = dis.readInt();
					int nr = dis.readInt();
					byte[] payload = readBytes(len, dis);
					QJobWaitGroup g = new QJobWaitGroup();
					ArrayList<QJob> r = new ArrayList<QJob>();

					for (int i = 0; i < nr; i++) {
						long fid = ByteBuffer.wrap(payload, i * 32, 8).getLong();
						double b = ByteBuffer.wrap(payload, i * 32 + 8, 8).getDouble();
						double e = ByteBuffer.wrap(payload, i * 32 + 16, 8).getDouble();
						long rid = ByteBuffer.wrap(payload, i * 32 + 24, 8).getLong();
						QJob job = 
								new QJob(QJob.QOp.QFACT, new QJobArgs(fid, b, e, rid));
						g.addToGroup(job);
						QExec.execJob(job);
					}
					len = 0;
					while (g.getSize() > 0) {
						QJob j = g.getAnyJob();
						List<GenericFact> gfs = (List<GenericFact>)j.r;
						r.add(j);
						if (gfs != null && gfs.size() > 0) {
							for (GenericFact gf : gfs) {
								len += gf.getLength();
							}
						}
						System.out.println("[INFO] " + j + " in " + 
								j.getLatency() + " ms");
					}
					dos.write(header);
					dos.writeInt(len);
					dos.writeInt(nr);
					for (QJob j : r) {
						List<GenericFact> gfs = (List<GenericFact>)j.r;
						for (GenericFact gf : gfs) {
							dos.writeLong(gf.fact_id);
							dos.writeLong(gf.attr);
							dos.writeLong(gf.facts.length);
							for (int i = 0; i < gf.facts.length; i++) {
								dos.writeLong(gf.facts[i]);
							}
						}
					}
					break;
				}
				case QueryType.QFACT_GRAPH:
				{
					// Search FACTs in depth network
					//
					// request : HEADER | LEN | NR | [{FACT_ID(L),b(D),e(D),R_ID(L)]
					// response: HEADER | LEN | NR | [{FACT_ID,ATTR,[FACT_ID]}](ziped)
					int len = dis.readInt();
					int nr = dis.readInt();
					byte[] payload = readBytes(len, dis);
					QJobWaitGroup g = new QJobWaitGroup();
					ArrayList<QJob> rj = new ArrayList<QJob>();
					ConcurrentHashMap<Long, GenericFact> rg = 
							new ConcurrentHashMap<Long, GenericFact>();
					HashSet<Long> rs = new HashSet<Long>();

					for (int i = 0; i < nr; i++) {
						long fid = ByteBuffer.wrap(payload, i * 32, 8).getLong();
						double b = ByteBuffer.wrap(payload, i * 32 + 8, 8).getDouble();
						double e = ByteBuffer.wrap(payload, i * 32 + 16, 8).getDouble();
						long rid = ByteBuffer.wrap(payload, i * 32 + 24, 8).getLong();
						QJob job = 
								new QJob(QJob.QOp.QFACT_GRAPH, new QJobArgs(fid, b, e, rid));
						g.addToGroup(job);
						QExec.execJob(job);
					}
					
					len += __qfact_graph(g, rj, rs, rg);
					
					System.out.println("TARGETSIZE=" + rs.size() + " len=" + len);
					
					dos.write(header);
					dos.writeInt(len);
					dos.writeInt(rs.size());
					
					for (Map.Entry<Long, GenericFact> je : rg.entrySet()) {
						GenericFact gf = je.getValue();
						dos.writeLong(gf.fact_id);
						dos.writeLong(gf.attr);
						dos.writeLong(gf.facts.length);
						for (int i = 0; i < gf.facts.length; i++) {
							dos.writeLong(gf.facts[i]);
						}
					}
					break;
				}
				}
			}
		} catch (EOFException e) {
			// socket close, it is ok
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				s.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private int __qfact_graph(QJobWaitGroup g, ArrayList<QJob> rj, HashSet<Long> rs,
			ConcurrentHashMap<Long, GenericFact> rg) {
		HashSet<QJob> nextJobs = new HashSet<QJob>();
		int len = 0;

		while (g.getSize() > 0) {
			QJob j = g.getAnyJob();
			List<GenericFact> gfs = (List<GenericFact>)j.r;
			
			rj.add(j);
			if (gfs != null && gfs.size() > 0) {
				for (GenericFact gf : gfs) {
					len += gf.getLength();
					rs.add(gf.fact_id);
					rg.putIfAbsent(gf.fact_id, gf);
					if (gf.facts != null && gf.facts.length > 0) {
						for (Long _fid : gf.facts) {
							// should do next layer job
							if (!rs.contains(_fid)) {
								nextJobs.add(
										new QJob(QJob.QOp.QFACT_GRAPH, 
												new QJobArgs(_fid, 
														j.args.b, 
														j.args.e, 
														j.args.rid)));
							}
						}
					}
				}
			}
			System.out.println("[INFO] " + j + " in " + j.getLatency() + " ms");
		}
		for (QJob job : nextJobs) {
			g.addToGroup(job);
			QExec.execJob(job);
		}
		if (nextJobs.size() > 0)
			return len + __qfact_graph(g, rj, rs, rg);
		else
			return len;
	}

	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	public byte[] readBytes(int count, InputStream istream) throws IOException {
		byte[] buf = threadLocalRecvBuffer.get();
		int n = 0;

		if (buf.length < count) {
			buf = new byte[count];
		}
		while (count > n) {
			n += istream.read(buf, n, count - n);
		}

		return buf;
	}

	public byte[] readBytesN(int count, InputStream istream) throws IOException {
		byte[] buf = new byte[count];
		int n = 0;

		while (count > n) {
			n += istream.read(buf, n, count - n);
		}

		return buf;
	}
}
