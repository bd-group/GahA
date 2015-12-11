package iie.gaha.loader;

import iie.gaha.common.RPoolProxy;
import iie.gaha.common.WBUser;
import iie.gaha.common.WBUserFact;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

public class SinaUserLoader {
	
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
		
		String iname = null;
		String uri = null;
		int verbose = 0;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h     : print this help.");
				System.out.println("-f     : input csv file name");
				System.out.println("-uri   : redis uri");
			}
			if (o.flag.equals("-f")) {
				// set input csv file name
				if (o.opt == null) {
					System.out.println("-f [INPUT_FILE_PATH_NAME]");
					System.exit(0);
				}
				iname = o.opt;
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
		}
		
		if (iname == null) {
			System.out.println("Expect input file pathname, use -f");
			System.exit(0);
		}
		if (uri == null) {
			System.out.println("Expect redis pool uri, use -uri");
			System.exit(0);
		}
		
		// create redis pool proxy
		RPoolProxy rpp = new RPoolProxy(null);
		try {
			rpp.init(uri);
		} catch (Exception e2) {
			e2.printStackTrace();
			System.exit(0);
		}
		
		SinaUserOperator suo = new SinaUserOperator(rpp);
		suo.setVerbose(verbose);
		
		// read in the json data
		FileReader fr = null;
		BufferedReader bfr = null;
		try {
			fr = new FileReader(new File(iname));
			bfr = new BufferedReader(fr);
			long begin = System.currentTimeMillis();
			long nr = 0, add = 0;
			while (true) {
				String line = bfr.readLine();
				if (line == null)
					break;
				nr++;
				WBUser wb = JSON.parseObject(line, WBUser.class);
				if (verbose >= 3) System.out.println(wb);
				if (wb.fui != null && wb.fui.length > 0) {
					suo.addFact(wb._id, 0D, new WBUserFact(wb._id, wb.fui, wb.fn));
					add++;
				}
			}
			suo.flush();
			long end = System.currentTimeMillis();
			System.out.println(
					String.format("Total %d facts, add %d in %d ms, OPS=%.4f", 
							nr, add, (end - begin), (1000.0 * add / (end - begin))));
			System.out.println(suo.getSendBytes());
			System.out.println(suo.getRedisCall());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bfr != null )
				try {
					bfr.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				};
		}
		
		rpp.quit();
	}

}
