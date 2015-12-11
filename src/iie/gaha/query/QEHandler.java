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
import java.util.List;

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
						System.out.println("[INFO] JOB " + j.jobId + " exec " + 
								j.status + " in " + j.getLatency() + " ms");
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
				case QueryType.QFACT_GRAPH:
					// request : HEADER | LEN | NR | [FACT_ID(long)]
					// response: HEADER
					break;
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
