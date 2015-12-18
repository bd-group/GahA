package iie.gaha.http;

import iie.gaha.client.QEClientCore;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.server.Server;

public class HTTPServer {
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
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
		int httpPort = 33033;
		int verbose = 0;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h     : print this help.");
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
			if (o.flag.equals("-p")) {
				// set http listen port
				if (o.opt == null) {
					System.out.println("-p HTTP_LISTEN_PORT");
					System.exit(0);
				}
				httpPort = Integer.parseInt(o.opt);
			}
		}
		
		if (uri == null) {
			System.out.println("Expect redis pool uri, use -uri");
			System.exit(0);
		}
		final QEClientCore qcc = new QEClientCore();
		try {
			qcc.init(uri);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		// Start HTTP server
		Server server = new Server(httpPort);
		server.setHandler(new HTTPHandler(qcc));
		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdown HTTP server, release resources.");
				qcc.quit();
			}
		});
	}

}
