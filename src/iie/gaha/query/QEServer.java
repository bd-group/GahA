package iie.gaha.query;

import iie.gaha.common.QEConf;
import iie.gaha.query.QJob.QJobArgs;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class QEServer {
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	public static String getHostIPByHint(String hint) throws SocketException {
		String node = null;
		
		Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
		while (e.hasMoreElements()) {
			NetworkInterface n = (NetworkInterface)e.nextElement();
			Enumeration<InetAddress> ee = n.getInetAddresses();
			while (ee.hasMoreElements()) {
				InetAddress i = (InetAddress)ee.nextElement();
				if (i.getHostAddress().contains(hint)) {
					node = i.getHostAddress();
					break;
				}
			}
			if (node != null)
				break;
		}
		
		return node;
	}

	/**
	 * @param args
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
		
		for (Option o : optsList) {
			if (o.flag.equals("-uip")) {
				try {
					nodeName = getHostIPByHint(outsideIP);
				} catch (SocketException e) {
					e.printStackTrace();
				}
			}
		}
		
		if (uri == null) {
			System.out.println("Expect redis pool uri, use -uri");
			System.exit(0);
		}
		if (outsideIP == null) {
			System.out.println("You HAVE TO set outside IP address (-ip IPADDR_HINT)(e.g. -ip .69.).");
			System.exit(0);
		} else {
			try {
				InetAddress[] a = InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
				for (InetAddress ia : a) {
					if (ia.getHostAddress().contains(outsideIP)) {
						System.out.println("[1] Got host IP " + ia.getHostAddress() + " by hint " + outsideIP);
						outsideIP = ia.getHostAddress();
						isSetOutsideIP = true;
					}
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.exit(0);
			}
			if (!isSetOutsideIP) {
				try {
					String hint = outsideIP;
					outsideIP = getHostIPByHint(hint);
					isSetOutsideIP = true;
					System.out.println("[2] Got host IP " + outsideIP + " by hint " + hint);
				} catch (SocketException e) {
					e.printStackTrace();
				}
			}
		}
		
		QE qe = null;
		try {
			QEConf conf = new QEConf();
			if (nodeName != null)
				conf.setNodeName(nodeName);
			if (isSetOutsideIP)
				conf.setOutsideIP(outsideIP);
			
			qe = new QE(conf);
			qe.init(uri);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		qe.startUp();
	}

}
