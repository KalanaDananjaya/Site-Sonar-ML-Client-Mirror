import lia.Monitor.JiniClient.Store.Main;

import lia.Monitor.monitor.monPredicate;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.eResult;
import lia.Monitor.monitor.ExtResult;
import lia.Monitor.monitor.AccountingResult;
import lia.Monitor.monitor.ShutdownReceiver;
import lia.util.ShutdownManager;

import lia.Monitor.monitor.DataReceiver;

import lia.Monitor.monitor.MFarm;

import lia.Monitor.DataCache.DataSelect;

import lia.Monitor.Store.TransparentStoreFast;
import lia.Monitor.Store.TransparentStoreFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Vector;
import java.util.Arrays;



public class SimpleClient {

    public static void main(String args[]){
	// start the repository service
	final Main jClient = new Main();
	
	// register a MyDataReceiver object to receive any new information
	// the implementation is just below, saves the values in the spool/ directory in files rotated at 15 minutes
	// to change the behaviour either implement a similar receiver or modify the addResult() methods below
	jClient.addDataReceiver(new MyDataReceiver());
	

    }
    
    /**
     * This is a very simple data receiver that puts some filters on the received data
     * and outputs the matching values on the console.
     */ 
    private static class MyDataReceiver implements DataReceiver, ShutdownReceiver {
	
	public MyDataReceiver(){
	    ShutdownManager.getInstance().addModule(this);
	}
	
	private File lastFile = null;
	
	private PrintWriter pw = null;
	
	private long lastRotated = 0;

	public void Shutdown(){
	    try{
		close();
	    }
	    catch (final IOException ioe){
		// ignore
	    }
	}
	
	private void close() throws IOException {
	    if (pw!=null){
	        pw.close();
		    
	        lastFile.renameTo(new File(lastFile.getCanonicalPath()+".done"));
	        
	        pw = null;
	    }
	}
	
	private void logLine(final String line) throws IOException {
	    if ( pw == null || (System.currentTimeMillis() - lastRotated > 1000*60*15 ) ){
		close();
	    
		lastRotated = System.currentTimeMillis();
		
		lastFile = new File("spool/"+lastRotated);
		    
		pw = new PrintWriter(lastFile);
	    }

	    pw.println(line);
	}
	
	private void logResult(final long timestamp, final String farm, final String cluster, final String node, final String parameter, final String value){
	    try{
		logLine(timestamp+"\t"+farm+"\t"+cluster+"\t"+node+"\t"+parameter+"\t"+value);
	    }
	    catch (final IOException ioe){
		System.err.println(ioe.getMessage());
	    }
	}
	
	public void addResult(eResult r){
		// Strings go here
	    // this is where injecting the received data in the target database should be done instead of logging it in a file
		DatabaseConnection db = new DatabaseConnection();
	    for (int i=0; i<r.param.length; i++){
			logResult(r.time, r.FarmName, r.ClusterName, r.NodeName, r.param_name[i], r.param[i].toString());
			db.addJobResults(r.NodeName, r.param_name[i], r.param[i].toString());
	    }
	}

	public void addResult(Result r){
	    // in this example there are no String values received, when subscribing to such values this should also be implemented
	    for (int i=0; i<r.param.length; i++)
		logResult(r.time, r.FarmName, r.ClusterName, r.NodeName, r.param_name[i], String.valueOf(r.param[i]));
	}
	
	public void addResult(ExtResult er){
	}
	
	public void addResult(AccountingResult ar){
	}
	
	public void updateConfig(MFarm f){
	}
    }
}
