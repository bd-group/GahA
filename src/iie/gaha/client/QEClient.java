package iie.gaha.client;

import iie.gaha.common.GenericFact;

import java.util.ArrayList;
import java.util.List;

public class QEClient {

	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
		List<Option> optsList = new ArrayList<Option>();
		List<String> doubleOptsList = new ArrayList<String>();

		// parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
			case '-':
				if (args[i].length() < 2)
					throw new IllegalArgumentException("Not a valid argument: "+args[i]);
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3)
						throw new IllegalArgumentException("Not a valid argument: "+args[i]);
					doubleOptsList.add(args[i].substring(2, args[i].length()));
				} else {
					if (args.length-1 > i)
						if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i+1]));
							i++;
						}
					else {
						optsList.add(new Option(args[i], null));
					}
				}
				break;
			default:
				// arg
				argsList.add(args[i]);
				break;
			}
		}
		
		String uri = null;
		String outsideIP = null;
		String nodeName = null;
		boolean isSetOutsideIP = false;
		int verbose = 0;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h     : print this help.");
				System.out.println("-uri   : redis uri");
			}
			if (o.flag.equals("-uri")) {
				// set redis pool uri
				if (o.opt == null) {
					System.out.println("-uri [REDIS_POOL_URI]");
					System.exit(0);
				}
				uri = o.opt;
			}
			if (o.flag.equals("-v")) {
				verbose++;
			}
			if (o.flag.equals("-vv")) {
				verbose += 2;
			}
			if (o.flag.equals("-vvv")) {
				verbose += 3;
			}
			if (o.flag.equals("-ip")) {
				// set outside accessible IP address hint
				if (o.opt == null) {
					System.out.println("-ip IPAddressHint");
					System.exit(0);
				}
				outsideIP = o.opt;
			}
		}
		
		if (uri == null) {
			System.out.println("Expect redis pool uri, use -uri");
			System.exit(0);
		}
		
		QEClientCore qcc = new QEClientCore();
		try {
			qcc.init(uri);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		try {
			List<GenericFact> gfs = qcc.qFact(49500, 0D, 0D, -1);
			if (gfs != null && gfs.size() > 0) {
				for (GenericFact gf : gfs) {
					System.out.println("ID " + gf.fact_id + 
							" follows=" + gf.facts.length + ", fans=" + gf.attr);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		qcc.quit();
		
		/*//long fid = 1095179000;
		long fid = 49500;
		long jobId = qe.submitToQjob(new QJob(QJob.QOp.QFACT,
				new QJobArgs(fid, 0D, 0D, -1)));

		QJob j = qe.getJob(jobId);
		if (j != null) {
			List<Long> ids = (List<Long>)j.r;
			for (Long id : ids) {
				System.out.print(id + ",");
			}
			System.out.println("\nID " + fid + " follows " + ids.size() + " user(s).");
		}
		System.out.println("END");
		*/
	}
}
