package iie.gaha.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import iie.gaha.common.GenericFact;
import iie.gaha.common.QueryFlag;
import iie.gaha.common.QueryType;
import iie.gaha.common.SocketHashEntry;

public class QEClientExec {
	
	public static List<GenericFact> __qFact(SocketHashEntry she, long fid,
			double b, double e, long rid) throws IOException {
		List<GenericFact> r = null;
		long id = she.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not find free socket for server: " +
					she.hostname + ":" + she.port);
		DataOutputStream storeos = she.map.get(id).dos;
		DataInputStream storeis = she.map.get(id).dis;
		
		// request : HEADER | LEN | NR | [{FACT_ID(L),b(D),e(D),R_ID(L)]
		// response: HEADER | LEN | NR | [{FACT_ID,ATTR,[FACT_ID]}](ziped)
		byte[] header = new byte[4];
		header[0] = QueryType.QFACT;
		header[1] = QueryFlag.ACCEPT_JAVA_OBJ;
		
		try {
			synchronized (storeos) {
				storeos.write(header);
				storeos.writeInt(32);
				storeos.writeInt(1);

				// Query args
				storeos.writeLong(fid);
				storeos.writeDouble(b);
				storeos.writeDouble(e);
				storeos.writeLong(rid);
				storeos.flush();
			}
			r = __handleInput4GFacts(storeis);
			she.setFreeSocket(id);
		} catch (Exception e1) {
			System.out.println("__qFact send/recv failed: " + e1.getMessage() + 
					" r?null=" + (r == null ? true : false));
			// remove this socket do reconnect?
			she.delFromSockets(id);
		}
		return r;
	}

	private static List<GenericFact> __handleInput4GFacts(
			DataInputStream dis) throws IOException {
		byte[] header = new byte[4];
		List<GenericFact> r = new ArrayList<GenericFact>();
		
		synchronized (dis) {
			dis.read(header);
			dis.readInt();
			int nr = dis.readInt();
			for (int i = 0; i < nr; i++) {
				GenericFact gf = new GenericFact();
				gf.fact_id = dis.readLong();
				gf.attr = dis.readLong();
				long fnr = dis.readLong();
				gf.facts = new long[(int) fnr];
				for (int j = 0; j < fnr; j++) {
					gf.facts[j] = dis.readLong();
				}
				r.add(gf);
			}
		}
		return r;
	}
}
