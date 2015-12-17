package iie.gaha.http;

import iie.gaha.client.QEClientCore;
import iie.gaha.common.GenericFact;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

public class HTTPHandler extends AbstractHandler {
	private QEClientCore qcc;
	
	public HTTPHandler(QEClientCore qcc) {
		this.qcc = qcc;
	}

	private void badResponse(Request baseRequest, HttpServletResponse response, 
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private void notFoundResponse(Request baseRequest, HttpServletResponse response, 
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private void okResponse(Request baseRequest, HttpServletResponse response, 
			byte[] content) throws IOException {
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getOutputStream().write(content);
		response.getOutputStream().flush();
	}
	
	private void __gen_qfact(long fid, PrintWriter out) {
		if (fid >= 0) {
			try {
				List<GenericFact> gfs = qcc.qFact(fid, 0, 0, -1);
				if (gfs != null && gfs.size() > 0) {
					out.write("{\"nodes\":[");
					for (GenericFact gf : gfs) {
						out.write(gf.fact_id+"");
						for (long _fid : gf.facts) {
							out.write("," + _fid);
						}
						System.out.println("ID " + gf.fact_id + 
								" follows=" + gf.facts.length + ", fans=" + gf.attr);
					}
					out.write("],\"edges\":{");
					for (int j = 0; j < gfs.size(); j++) {
						GenericFact gf = gfs.get(j);
						out.write("\"" + gf.fact_id);
						out.write("\":[");
						for (int i = 0; i < gf.facts.length; i++) {
							out.write(gf.facts[i] + "");
							if (i != gf.facts.length - 1) 
								out.write(",");
						}
						out.write("]");
						if (j != gfs.size() - 1)
							out.write(",");
					}
					out.write("}}");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void __gen_qfacts(long fid, PrintWriter out) {
		if (fid >= 0) {
			try {
				List<GenericFact> gfs = qcc.qFactGraph(fid, 0, 0, -1);
				if (gfs != null && gfs.size() > 0) {
					out.write("{\"nodes\":[");
					for (GenericFact gf : gfs) {
						out.write(gf.fact_id+"");
						for (long _fid : gf.facts) {
							out.write("," + _fid);
						}
						System.out.println("ID " + gf.fact_id + 
								" follows=" + gf.facts.length + ", fans=" + gf.attr);
					}
					out.write("],\"edges\":{");
					for (int j = 0; j < gfs.size(); j++) {
						GenericFact gf = gfs.get(j);
						out.write("\"" + gf.fact_id);
						out.write("\":[");
						for (int i = 0; i < gf.facts.length; i++) {
							out.write(gf.facts[i] + "");
							if (i != gf.facts.length - 1) 
								out.write(",");
						}
						out.write("]");
						if (j != gfs.size() - 1)
							out.write(",");
					}
					out.write("}}");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void __gen_qfact_random(int tnr, PrintWriter out) {
		List<Long> fids = qcc.__random_keys(tnr);
		List<GenericFact> gfs = new ArrayList<GenericFact>();
		
		for (long fid : fids) { 
			try {
				List<GenericFact> __gfs = qcc.qFact(fid, 0, 0, -1);
				gfs.addAll(__gfs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (gfs != null && gfs.size() > 0) {
			boolean isFirst = true;
			out.write("{\"nodes\":[");
			for (GenericFact gf : gfs) {
				if (isFirst) {
					out.write(gf.fact_id+"");
					isFirst = false;
				} else {
					out.write("," + gf.fact_id);
				}
				for (long _fid : gf.facts) {
					out.write("," + _fid);
				}
				System.out.println("RANDOM ID " + gf.fact_id + 
						" follows=" + gf.facts.length + ", fans=" + gf.attr);
			}
			out.write("],\"edges\":{");
			for (int j = 0; j < gfs.size(); j++) {
				GenericFact gf = gfs.get(j);
				out.write("\"" + gf.fact_id);
				out.write("\":[");
				for (int i = 0; i < gf.facts.length; i++) {
					out.write(gf.facts[i] + "");
					if (i != gf.facts.length - 1) 
						out.write(",");
				}
				out.write("]");
				if (j != gfs.size() - 1)
					out.write(",");
			}
			out.write("}}");
		}
	}
	
	private void doGaha(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String factid = request.getParameter("factid");
		long fid = -1;
		
		try {
			fid = Long.parseLong(factid);
		} catch (Exception e) {
		}

		if (target.startsWith("/gaha/qfacts")) {
			PrintWriter out = response.getWriter();
		
			out.write(request.getParameter("callback") + "(");
			__gen_qfacts(fid, out);
			out.write(");");
			out.flush();
			out.close();
			
			return;
		} else if (target.startsWith("/gaha/qfactR")) {
			PrintWriter out = response.getWriter();
		
			out.write(request.getParameter("callback") + "(");
			__gen_qfact_random((int)fid, out);
			out.write(");");
			out.flush();
			out.close();
			
			return;
		} else if (target.startsWith("/gaha/qfact")) {
			PrintWriter out = response.getWriter();
		
			out.write(request.getParameter("callback") + "(");
			__gen_qfact(fid, out);
			out.write(");");
			out.flush();
			out.close();
			
			return;
		}
		// default response for /gaha/
		ResourceHandler rh = new ResourceHandler();
		rh.setResourceBase(".");
		rh.handle(target, baseRequest, request, response);
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		try {
			if (target == null) {
				// bad response
				badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
			} else if (target.startsWith("/gaha/")) {
				doGaha(target, baseRequest, request, response);
			}
		} catch (Exception e) {
			e.printStackTrace();
			badResponse(baseRequest, response, 
					"#FAIL: internal error: " + e.getMessage());
		}
	}
}
