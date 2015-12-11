package iie.gaha.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WBUser {
	// message number
	public long mn;
	// screen name
	public String sn;
	// profile image url
	public String iu;
	// is verified
	public boolean iv;
	// insert time
	public String wt;
	// user name
	public String un;
	// follower_userid
	public String[] fui;
	// description
	public String de;
	// user id
	public String _id;
	// attention number
	public long an;
	// address
	public String ad;
	// fans number
	public long fn;
	// sex
	public String sx;
	// is daren
	public String dr;
	// verify info
	public String vi;
	// user tag, splited by ,
	public String tg;
	// education info
	public String ei;
	// career info
	public String ci;
	// base info 
	public String bi;
	// create time
	public String at;
	
	public long getWBAgeInDays() {
		if (at != null) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			Date d;
			try {
				d = df.parse(at);
			} catch (ParseException e) {
				e.printStackTrace();
				return -1L;
			}
			return (System.currentTimeMillis() - d.getTime()) / 86400000;
		} else 
			return -1L;
	}
	
	public String toString() {
		return "UID " + _id + " NAME " + un + " SEND " + mn + " FANS " + fn + 
				" FLLW " + (fui != null ? fui.length : 0) + " DAYS " + getWBAgeInDays();
	}
}
