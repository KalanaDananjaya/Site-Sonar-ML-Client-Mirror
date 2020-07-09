/*
 * Created on Nov 20, 2003
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import lia.Monitor.monitor.AppConfig;
import lia.Monitor.monitor.DataReceiver;
import lia.Monitor.monitor.ExtResult;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.AccountingResult;
import lia.Monitor.monitor.eResult;

import lia.util.ntp.NTPDate;

import lia.util.BoundedDropVector;
import lia.util.DropEvent;

public class ResultFileLogger extends Thread implements DataReceiver, DropEvent {

	/** Logger name */
	private static final String				COMPONENT	= "lia.Monitor.JiniClient.Store";

	/** Logger used by this class */
	private static final Logger				logger		= Logger.getLogger(COMPONENT);

	private static Calendar					cal			= Calendar.getInstance();

	private BoundedDropVector				buff;

	private Vector<Object>					tmpbuff;

	public boolean						hasToRun;

	private static final SimpleDateFormat	dateform	= new SimpleDateFormat(" HH:mm:ss");

	private static NumberFormat				nf			= NumberFormat.getInstance();

	private static String					dirPath		= AppConfig.getProperty("lia.Monitor.JStore.DirLogger", ".");

	private String						cfn			= null;

	private int						iMaxDays	= -1;

	static ResultFileLogger					rflInstance	= null;

	public static final synchronized ResultFileLogger getLoggerInstance() {
		if (rflInstance==null){
			try{
				rflInstance = new ResultFileLogger();
				rflInstance.start();
			}
			catch (Exception e){
				rflInstance = null;
			}
		}
			
		return rflInstance;
	}

	public boolean isActive() {
		return hasToRun;
	}
	
	private ResultFileLogger() {
		super("( ML ) JStore Results File Logger");

		this.iMaxDays = AppConfig.geti("lia.Monitor.Store.FileLogger.maxDays", 2);

		if (iMaxDays<=0)
			throw new IndexOutOfBoundsException("lia.Monitor.Store.FileLogger.maxDays is <=0, file logging is disabled");
		
		hasToRun = true;
		buff = new BoundedDropVector(AppConfig.geti("lia.Monitor.JStore.BufferSize", 10000), this);
		tmpbuff = new Vector<>();
		nf.setMaximumFractionDigits(3);
		nf.setMinimumFractionDigits(1);
		nf.setGroupingUsed(false);

		cleanupDir(iMaxDays);
	}

	private int iDropEvents = 0;
	
	public void notifyDrop() {
		synchronized (buff) {
			iDropEvents++;
			
			if (iDropEvents>100){
				logger.log(Level.WARNING, "ResultFileLogger is stoping now because there are too many drop events");

				hasToRun = false;
			}
			else{
				logger.log(Level.WARNING, "ResultFileLogger received notification no. "+iDropEvents+" that the buffer is full");
			}
			
			buff.clear();
		}
	}

	public void addResult(Object o) {
		if (o instanceof Result)
			addResult((Result) o);
		else
		if (o instanceof eResult)
			addResult((eResult) o);
		else 
		if (o instanceof Collection)
			addResult((Collection) o);
	}

	public void addResult(Collection c) {
		if (c == null || c.size() <= 0)
			return;

		if (hasToRun){
			Iterator it = c.iterator();
		
			while (it.hasNext()){
				addResult(it.next());
			}
		}
	}

	public void addResult(Result a) {
		if (hasToRun)
			buff.add(a);
	}

	public void addResult(eResult a) {
		if (hasToRun)
			buff.add(a);
	}

	public void addResult(ExtResult a) {

	}

	public void addResult(AccountingResult a) {

	}

	public void updateConfig(lia.Monitor.monitor.MFarm f) {

	}

	private static String formatResult(Result r) {
		if (r == null)
			return null;
		
		if (r.param == null || r.param.length == 0 || r.param_name == null || r.param_name.length == 0)
			return null;

		final StringBuffer sb = new StringBuffer(100);

		final long time = r.time;
		final Date da = new Date(time);
		final String date = dateform.format(da);

		for (int i = 0; i < r.param_name.length; i++) {
			//ans += date + "  " + r.ClusterName + "  "+ r.NodeName+ "  " + r.param_name[i] + " "+ nf.format(r.param[i]) + "\n"; 
			sb.append(date).append('\t');
			sb.append(r.FarmName).append('\t');
			sb.append(r.ClusterName).append('\t');
			sb.append(r.NodeName).append('\t');
			sb.append(r.param_name[i]).append('\t');
			sb.append(nf.format(r.param[i])).append('\n');
		}

		return sb.toString();
	}

	private static String formatResult(eResult r) {
		if (r == null)
			return null;
		
		if (r.param == null || r.param.length == 0 || r.param_name == null || r.param_name.length == 0)
			return null;

		final StringBuffer sb = new StringBuffer(100);

		final long time = r.time;
		final Date da = new Date(time);
		final String date = dateform.format(da);

		for (int i = 0; i < r.param_name.length; i++) {
			//ans += date + "  " + r.ClusterName + "  "+ r.NodeName+ "  " + r.param_name[i] + " "+ nf.format(r.param[i]) + "\n"; 
			sb.append(date).append('\t');
			sb.append(r.FarmName).append('\t');
			sb.append(r.ClusterName).append('\t');
			sb.append(r.NodeName).append('\t');
			sb.append(r.param_name[i]).append('\t');
			if (r.param[i]==null)
				sb.append("NULL");
			else
				sb.append(r.param[i].getClass().getName()).append(':').append(r.param[i].toString());
			
			sb.append('\n');
		}

		return sb.toString();
	}

	
	private static final void zipFile(String dir, String fileName) throws Exception {
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(dir + "/" + fileName + ".zip"));
		FileInputStream fis = new FileInputStream(dir + "/" + fileName);
		byte[] buff = new byte[1024];

		zos.putNextEntry(new ZipEntry(fileName));

		boolean hasToRead = true;
		while (hasToRead) {
			int n = fis.read(buff, 0, buff.length);
			if (n > 0) {
				zos.write(buff, 0, n);
			} else {
				hasToRead = false;
			}
		}

		zos.flush();

		fis.close();
		zos.close();
		logger.log(Level.INFO, "\n[ ZIP ]\nFrom: " + fileName + " [ " + new File(dir + "/" + fileName).length() + " ]\nTo: " + fileName + ".zip [ " + new File(dir + "/" + fileName).length() + " ]\n");
	}

	private long					lLastSaved		= NTPDate.currentTimeMillis();

	private static final Pattern	LOGFILE_PATTERN	= Pattern.compile(".*JStore_([0-9]+)_([0-9]+)_([0-9]+)\\.log.*");

	public static final void cleanupDir(int iMaxDays) { // delete old log files
		//logger.log(Level.INFO, "CleaupDir : "+iMaxDays);

		if (iMaxDays <= 0)
			return;

		try {
			File f = new File(dirPath);

			String[] fileList = f.list();

			if (fileList == null || fileList.length == 0)
				return;

			Matcher m;

			long now = NTPDate.currentTimeMillis();

			for (int i = 0; i < fileList.length; i++) {
				String sFN = fileList[i];

				//System.err.println("FN : "+sFN);

				m = LOGFILE_PATTERN.matcher(sFN);

				if (m.matches()) {
					//System.err.println("  Matches");
					String sYear = m.group(1);
					String sMonth = m.group(2);
					String sDay = m.group(3);

					Date d = new Date(Integer.parseInt(sYear) - 1900, Integer.parseInt(sMonth) - 1, Integer.parseInt(sDay), 0, 0, 0);

					//System.err.println(now+" : "+d+" : "+(now - d.getTime()));

					if (now - d.getTime() > ((long) (iMaxDays + 1)) * 24L * 60L * 60L * 1000L) {
						//System.err.println("  DELETE");
						try {
							(new File(dirPath + "/" + sFN)).delete();
						} catch (Exception ee) {
							logger.log(Level.WARNING, "Cannot delete : " + sFN, ee);
						}
					} else {
						//System.err.println("  KEEP");
					}
				} else {
					//System.err.println("  Doesn't match");
				}
			}

		} catch (Exception e) {
			logger.log(Level.WARNING, "ResultFileLogger : exception during cleanup", e);
		}

	}

	private void writeResults() {
		if (buff.size() == 0)
			return;

		lLastSaved = NTPDate.currentTimeMillis();

		synchronized (buff) {
			tmpbuff.clear();
			tmpbuff.addAll(buff);
			buff.clear();
			
			// each successfull write decreases the stop counter
			if (iDropEvents>0)
				iDropEvents--;
		}

		try {
			cal.setTimeInMillis(NTPDate.currentTimeMillis());
			int iyear = cal.get(Calendar.YEAR);
			int imonth = cal.get(Calendar.MONTH) + 1;
			String month = (imonth < 10) ? ("0" + imonth) : "" + imonth;
			int iday = cal.get(Calendar.DAY_OF_MONTH);
			String day = (iday < 10) ? ("0" + iday) : "" + iday;
			String ccfn = "JStore_" + iyear + "_" + month + "_" + day + ".log";
			if (cfn == null) {
				cfn = ccfn;
			} else {
				if (cfn.compareTo(ccfn) != 0) {//should "zip" the older file
					logger.log(Level.INFO, "START logrotate");
					boolean bDeleteAfterZip = true;
					try {
						if (AppConfig.getb("lia.Monitor.Store.ResultFileLogger.zip_old_files", true))
							zipFile(dirPath, cfn);
						else
							bDeleteAfterZip = false;
					} catch (Throwable t) {
						bDeleteAfterZip = false;
						logger.log(Level.WARNING, " Got error trying to zip file: " + cfn + " to directory: " + dirPath);
					}

					if (bDeleteAfterZip) {
						try {
							new File(dirPath + "/" + cfn).delete();
						} catch (Throwable t1) {
							logger.log(Level.WARNING, " Exception Got Trying to delete the file: " + dirPath + "/" + cfn);
						}
					}
					cfn = ccfn;

					cleanupDir(iMaxDays);

					logger.log(Level.INFO, "END logrotate");
				}
			}

			final File f = new File(dirPath + "/" + cfn);
			final BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));

			for (int i = 0; i < tmpbuff.size(); i++) {
				Object o = tmpbuff.elementAt(i);
				if (o != null) {
					String toWrite = null;
					
					if (o instanceof Result) {
						toWrite = formatResult((Result) o);
					}
					else
					if (o instanceof eResult){
						toWrite = formatResult((eResult) o);
					}
					
					if (toWrite != null && toWrite.length() > 0) {
						if (logger.isLoggable(Level.FINEST)) {
							logger.log(Level.FINER, " Writing " + toWrite);
						}
						bw.write(toWrite);
					}
				}
			}
			bw.flush();
			bw.close();
		}
		catch (Throwable t) {

		}
	}

	public void run() {
		logger.log(Level.INFO, "ResultFileLogger STARTED. Logging dir = " + dirPath);

		try {
			// try to create the destination folder
			(new File(dirPath)).mkdirs();
		} catch (Exception e) {
		}

		while (hasToRun) {
			try {
				try {
					Thread.sleep(1000);
				}
				catch (Exception e) {
				}
				
				if (logger.isLoggable(Level.FINER)) {
					logger.log(Level.FINER, "\n\n Got " + buff.size());
				}
				
				if (buff.size() > 100 || (buff.size() > 1 && NTPDate.currentTimeMillis() - lLastSaved > 30 * 1000)) {
					writeResults();
				}

			}
			catch (Throwable t) {
				logger.log(Level.WARNING, COMPONENT + " Got Exception in main loop " + t);
			}
		}

	}

	public static final void main(String[] args) {
		try {
			ResultFileLogger.zipFile("/home/ramiro/JStoreLogger", "JStore_2003_11_20.log");

			cleanupDir(2);
		} catch (Throwable t) {
			logger.log(Level.WARNING, " Exc ", t);
		}
	}
}
