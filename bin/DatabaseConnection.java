import lia.Monitor.Store.Fast.DB;

import java.util.ArrayList;

import org.apache.commons.lang3.StringEscapeUtils;

public class DatabaseConnection {
    DB conn;
    int run_id;
    int run_expiration_time = 1; // Mark run as expired after this time period
    int site_expiration_time = 1; // Mark site as STALLED if no update has happened for this time period

    /**
     * Connect to the database
     */
    public DatabaseConnection() {

        this.conn = new DB();
        String sql = "SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1";
        if (this.conn.query(sql)){
            if (conn.moveNext()){
                this.run_id =  conn.geti("run_id");
            }
        }

    }


    public Integer getSiteIdByJobId(Integer jobId){

        Integer siteId = -1;
        String sql = "SELECT site_id FROM jobs WHERE job_id = %d";
        String preparedStmt = String.format(sql,jobId);
        if (this.conn.query(preparedStmt)){
            if (conn.moveNext()){
                siteId =  conn.geti("site_id");
            }
        }
        System.err.println("Site id: " + siteId + " for job id: " + jobId);
        return siteId;
    }


    public Integer addNode(Integer siteId, String nodeName){
        String sql = "INSERT INTO nodes (site_id,run_id,node_name) VALUES (%d, %d, '%s')";
        String preparedStmt = String.format(sql,siteId,this.run_id,nodeName);
        Integer nodeId = -1;
       
        if (this.conn.query(preparedStmt)){
            if (this.conn.query("SELECT LAST_INSERT_ID()")){
                if (conn.moveNext()){
                    nodeId =  Integer.parseInt(conn.gets(1));
                }
            }
        }
        System.err.println("New node entry with id " + nodeId + " created succefully for " + nodeName);
        return nodeId;
    }


    public Integer getNodeIdByNodeName(Integer siteId, String nodeName){
        System.err.println("Get node by name called");
        String sql = "SELECT node_id FROM nodes WHERE (site_id=%d) AND (node_name='%s') AND (run_id=%d)";
        String preparedStmt = String.format(sql,siteId,nodeName,this.run_id);
        Integer nodeId = 0;

        if (this.conn.query(preparedStmt)){
            if (!conn.moveNext()){
                nodeId =  -1; // Node does not exist
                System.err.println("No data");
            }
            else{
                nodeId =  Integer.parseInt(conn.gets(1));
            }
        }
        System.err.println("Id of node " + nodeName + " : "+ nodeId);
        return nodeId;
    }

    public void updateRunState(String state, int run_id ){
        System.err.println("updating run state");
        String sql = "UPDATE run SET state='%s' WHERE (run_id=%d)";
        String preparedStmt = String.format(sql,state,run_id);
        if (this.conn.query(preparedStmt)){
            System.err.println("Run "+ run_id + " marked as " + state);
        }
    }

    public void markStalledJobs(int run_id){
        String sql = "SELECT site_id FROM processing_state WHERE run_id=%d AND (TIMESTAMPDIFF(MINUTE,last_update,NOW())>%d)";
        String preparedStmt = String.format(sql,run_id,this.site_expiration_time);
        ArrayList<String> stalled_site_ids = new ArrayList<>();
        if (this.conn.query(preparedStmt)) {
            while (conn.moveNext()) {
                stalled_site_ids.add(this.conn.gets(1));
            }
        }
        for (int i=0;i<stalled_site_ids.size();i++){
            sql = "UPDATE processing_state SET state='%s' WHERE site_id=%d AND run_id=%d";
            String id = stalled_site_ids.get(i);
            preparedStmt = String.format(sql, "STALLED", Integer.parseInt(id), run_id);
            if (this.conn.query(preparedStmt)){
                System.err.println("Site " + id + " marked as STALLED");
            }
        }
    }

    public void manageRunState(int run_id){
        System.err.println("checking run state");
        // Update Stalled jobs
        markStalledJobs(run_id);

        // If no sites are waiting, mark the current run as completed
        ArrayList<String> waiting_site_list = getSitesByProcessingState("WAITING", run_id);
        if (waiting_site_list.size() == 0){
            updateRunState("COMPLETED", run_id);
            // TODO: Exit the program
        }
        else{
            //If sites are waiting but run has been running for more than 24 hours, mark run as complete
            String run_sql = "UPDATE run SET state='%s' WHERE (TIMESTAMPDIFF(MINUTE,last_update,NOW())>%d) AND run_id=%d";
            String run_preparedStmt = String.format(run_sql,"TIMED_OUT",this.run_expiration_time, run_id);
            if (this.conn.query(run_preparedStmt)){
                if (this.conn.getUpdateCount() > 0){
                    System.err.println("Run "+ run_id + " marked as TIMED_OUT due to exceeding "+ this.run_expiration_time);
                    // TODO: Exit the program
                }
            }
        }
    }

    public ArrayList<String> getSitesByProcessingState(String state, int run_id){
        String sql = "SELECT site_id FROM processing_state WHERE (state='%s') AND (run_id=%d)";
        String preparedStmt = String.format(sql,state,run_id);
        ArrayList<String> site_id_list = new ArrayList<String>();
        if (this.conn.query(preparedStmt)){
            while (conn.moveNext()){
                site_id_list.add(this.conn.gets(1));
            }
        }
        return site_id_list;
    }
    public void updateProcessingState(Integer siteId,int run_id){
        ArrayList<String> started_jobs = getAllJobsByStateAndSite(run_id,"STARTED", siteId);
        ArrayList<String> completed_jobs = getAllJobsByStateAndSite(run_id,"COMPLETED", siteId);
        ArrayList<String> killed_jobs = getAllJobsByStateAndSite(run_id,"KILLED", siteId);
        Integer len_started_jobs = started_jobs.size();
        Integer len_completed_jobs = completed_jobs.size();
        Integer len_killed_jobs = killed_jobs.size();
        
        Integer finished_job_count = len_completed_jobs+len_killed_jobs;
        int all_job_count = finished_job_count + len_started_jobs;
        String preparedStmt = "";
        if (((float) finished_job_count/(float) all_job_count) > 0.9){
            String sql = "UPDATE processing_state SET state='%s',running_job_count=%d,completed_job_count=%d,killed_job_count=%d,last_update=NOW() WHERE (site_id=%d) AND (run_id=%d)";
            preparedStmt = String.format(sql,"COMPLETED",len_started_jobs,len_completed_jobs,len_killed_jobs,siteId,run_id);
            if (this.conn.query(preparedStmt)){
                System.err.println("Processing State of site Id " + siteId + " in Run " + run_id + "updated.");
            }
            manageRunState(run_id);
        }
        else{
            String sql = "UPDATE processing_state SET running_job_count=%d,completed_job_count=%d,killed_job_count=%d,last_update=NOW() WHERE (site_id=%d) AND (run_id=%d)";
            preparedStmt = String.format(sql,len_started_jobs,len_completed_jobs,len_killed_jobs,siteId,run_id);
            if (this.conn.query(preparedStmt)){
                System.err.println("Processing State of site Id " + siteId + " in Run " + run_id + "updated.");
            }
        }


    }

    public ArrayList<String> getAllJobsByStateAndSite(int run_id, String state, Integer siteId){
        String sql = "SELECT job_id FROM jobs WHERE (job_state='%s') AND (run_id=%d) AND (site_id=%d)";
        String preparedStmt = String.format(sql,state,run_id,siteId);
        ArrayList<String> job_id_list = new ArrayList<String>();
        if (this.conn.query(preparedStmt)){
            while (conn.moveNext()){
                job_id_list.add(this.conn.gets(1));
            }
        }
        return job_id_list;
    }
    

    public void updateJobState(Integer jobId,String state,Integer siteId,int run_id){
        String sql = "UPDATE jobs SET job_state='%s'" +
                ",last_update=NOW() WHERE job_id='%s'";
        String preparedStmt = String.format(sql,state,jobId);
        if (this.conn.query(preparedStmt)){
          System.err.println("Job " + jobId + "marked as complete");
        }
        updateProcessingState(siteId,run_id);
    }

    public void addJobResults(String entryName, String paramName, String paramValue) {
        Integer jobId = Integer.parseInt(entryName.split("_")[0]);
        String nodeName = entryName.split("_")[1];
        Integer siteId = getSiteIdByJobId(jobId);
        Integer nodeId = getNodeIdByNodeName(siteId, nodeName);
        if (nodeId == -1 ){
            // If node does not exist, create Node
            nodeId = addNode(siteId, nodeName);
        }

        if ((paramName.equals("state")) && (paramValue.equals("complete"))){
            updateJobState(jobId,"COMPLETED",siteId,this.run_id);
        }
        else{
            String sql = "INSERT INTO parameters (job_id,run_id,node_id,paramName,paramValue,last_update) VALUES(%d, %d, %d, '%s','%s', NOW()) ON DUPLICATE KEY UPDATE job_id=job_id";
            String preparedStmt = String.format(sql,jobId,this.run_id,nodeId, StringEscapeUtils.escapeJava(paramName),StringEscapeUtils.escapeJava(paramValue));
            if (this.conn.query(preparedStmt)){
                System.err.println("New parameter added. Job ID: " + jobId + " Name: " + paramName + " Value:" + paramValue
                        + " Node Id: " + nodeId + " Site ID:" + siteId + " Run ID:" + run_id);
            };
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

    }

}
