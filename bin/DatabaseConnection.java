

import lia.Monitor.Store.Fast.DB;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.*;

import org.apache.commons.lang3.StringEscapeUtils;


public class DatabaseConnection {
    DB conn;
    int run_id;
    // Mark run as expired after this time period(hours)
    int run_expiration_time = 12;
    // Mark site as STALLED if no update has happened for this time period(hours)
    int site_expiration_time = 3;

    private final static Logger logger = Logger.getLogger(DatabaseConnection.class.getName());
    private static FileAppender handler;
    private static final ConsoleAppender consoleHandler = new ConsoleAppender(new PatternLayout("%d %-5p [%c{1}] %m%n"));


    static {
        try {
            handler = new FileAppender(new PatternLayout("%d %-5p [%c{1}] %m%n"), "logs/site_sonar_ML_client.log", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Connect to the database
     */
    public DatabaseConnection() {
        this.conn = new DB();
        // Get Current Run ID
        String sql = "SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1";
        if (this.conn.query(sql)) {
            if (conn.moveNext()) {
                this.run_id = conn.geti("run_id");
                logger.debug("Run Id retrieved from the database: " + this.run_id);
            }
        }
        else {
            logger.error("Unable to retreive Run Id");
        }
        logger.setLevel(Level.ALL);
        consoleHandler.activateOptions();
        handler.activateOptions();
        Logger.getRootLogger().addAppender(handler);
        Logger.getRootLogger().addAppender(consoleHandler);
        logger.info("Job parameter received");

    }


    public Integer getSiteIdByJobId(Integer jobId) {
        Integer siteId = -1;
        String sql = "SELECT site_id FROM jobs WHERE job_id = %d";
        String preparedStmt = String.format(sql, jobId);
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            if (conn.moveNext()) {
                siteId = conn.geti("site_id");
                String logMsg = "Site id: %s retrieved for job id: %d";
                logger.debug(String.format(logMsg, siteId, jobId));

            } else {
                String logMsg = "Site id for job id %d cannot be found";
                logger.warn(String.format(logMsg, siteId));

            }
        }
        else {
            logger.error(String.format("Unable to retrieve Site Id from Job Id. \nArgs - JobId: %d",jobId));
        }
        return siteId;
    }


    public Integer addNode(Integer siteId, String nodeName) {
        String sql = "INSERT INTO nodes (site_id,run_id,node_name) VALUES (%d, %d, '%s')";
        String preparedStmt = String.format(sql, siteId, this.run_id, nodeName);
        Integer nodeId = -1;
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            if (this.conn.query("SELECT LAST_INSERT_ID()")) {
                if (conn.moveNext()) {
                    nodeId = Integer.parseInt(conn.gets(1));
                    String logMsg = "New node entry with id %d created successfully for %s in site %d";
                    logger.info(String.format(logMsg, nodeId, nodeName, siteId));
                } else {
                    String logMsg = "Failed to retrieve the Id of last created node for %s";
                    logger.warn(String.format(logMsg, nodeName));
                }

            }
        }
        else {
            logger.error(String.format("Failed to create new node. \nArgs - siteId: %d\t nodeName: %s",siteId,nodeName));
        }
        return nodeId;
    }


    public Integer getNodeIdByNodeName(Integer siteId, String nodeName) {
        String sql = "SELECT node_id FROM nodes WHERE (site_id=%d) AND (node_name='%s') AND (run_id=%d)";
        String preparedStmt = String.format(sql, siteId, nodeName, this.run_id);
        Integer nodeId = 0;
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            if (!conn.moveNext()) {
                nodeId = -1; // Node does not exist
                String logMsg = "No node exist for node name: %s";
                logger.debug(String.format(logMsg, nodeName));

            } else {
                nodeId = Integer.parseInt(conn.gets(1));
                String logMsg = "Node id: %d for node name: %s retrieved";
                logger.debug(String.format(logMsg, nodeId, nodeName));

            }
        }
        else {
            logger.error(String.format("Unable to retrieve node Id by name. \nArgs - siteId: %d\t nodeName: %s",siteId,nodeName));
        }
        return nodeId;
    }

    public void updateRunState(String state, int run_id) {
        String sql = "UPDATE run SET state='%s' WHERE (run_id=%d)";
        String preparedStmt = String.format(sql, state, run_id);
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            String logMsg = "Run %d marked as %s";
            logger.info(String.format(logMsg, run_id, state));

        }
        else {
            logger.error(String.format("Unable to update Run state. \nArgs - state: %s\t runId: %d",state,run_id));
        }
    }

    public void markStalledJobs(int run_id) {
        String sql = "SELECT site_id FROM processing_state WHERE run_id=%d AND (TIMESTAMPDIFF(HOUR,last_update,NOW())>%d)";
        String preparedStmt = String.format(sql, run_id, this.site_expiration_time);
        ArrayList<String> stalled_site_ids = new ArrayList<>();
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            while (conn.moveNext()) {
                stalled_site_ids.add(this.conn.gets(1));
            }
        }
        if (stalled_site_ids.size() == 0) {
            String logMsg = "No stalled sites";
            logger.debug(logMsg);

        } else {
            for (int i = 0; i < stalled_site_ids.size(); i++) {
                sql = "UPDATE processing_state SET state='%s' WHERE site_id=%d AND run_id=%d";
                String id = stalled_site_ids.get(i);
                preparedStmt = String.format(sql, "STALLED", Integer.parseInt(id), run_id);
                logger.trace(preparedStmt);
                if (this.conn.query(preparedStmt)) {
                    String logMsg = "Site %d marked as STALLED";
                    logger.info(String.format(logMsg, Integer.parseInt(id)));

                }
                else {   
                    String logMsg = "Unable to mark Site %d marked as STALLED";
                    logger.error(String.format(logMsg, Integer.parseInt(id)));
                }
            }
        }
    }

    public void manageRunState(int run_id) {
        // Update Stalled jobs
        markStalledJobs(run_id);

        // If no sites are waiting, mark the current run as completed
        ArrayList<String> waiting_site_list = getSitesByProcessingState("WAITING", run_id);
        if (waiting_site_list.size() == 0) {
            updateRunState("COMPLETED", run_id);
            String logMsg = "Exiting...";
            logger.info(logMsg);

            // Exit the program
            System.exit(0);
        } else {
            //If sites are waiting but run has been running for more than 24 hours, mark run as complete
            String sql = "UPDATE run SET state='%s' WHERE (TIMESTAMPDIFF(HOUR,last_update,NOW())>%d) AND run_id=%d";
            String preparedStmt = String.format(sql, "TIMED_OUT", this.run_expiration_time, run_id);
            logger.trace(preparedStmt);
            if (this.conn.query(preparedStmt)) {
                if (this.conn.getUpdateCount() > 0) {
                    String logMsg = "Run %d marked as TIMED_OUT due to exceeding %d. Exiting...";
                    logger.info(String.format(logMsg, run_id, this.run_expiration_time));

                    // Exit the program
                    System.exit(0);
                }
            }
            else{
                logger.error(String.format("Unable to update run state. \nArgs - runId: %d",run_id));
            }
            
        }
    }

    public ArrayList<String> getSitesByProcessingState(String state, int run_id) {
        String sql = "SELECT site_id FROM processing_state WHERE (state='%s') AND (run_id=%d)";
        String preparedStmt = String.format(sql, state, run_id);
        logger.trace(preparedStmt);
        ArrayList<String> site_id_list = new ArrayList<String>();
        if (this.conn.query(preparedStmt)) {
            while (conn.moveNext()) {
                site_id_list.add(this.conn.gets(1));
            }
        }
        else{
            logger.error(String.format("Unable to retrieve site by processing state. \nArgs - state: %s\t runId: %d",state,run_id));
        }
        return site_id_list;
    }

    public void updateProcessingState(Integer siteId, int run_id) {
        ArrayList<String> started_jobs = getAllJobsByStateAndSite(run_id, "STARTED", siteId);
        ArrayList<String> completed_jobs = getAllJobsByStateAndSite(run_id, "COMPLETED", siteId);
        ArrayList<String> killed_jobs = getAllJobsByStateAndSite(run_id, "KILLED", siteId);
        Integer len_started_jobs = started_jobs.size();
        Integer len_completed_jobs = completed_jobs.size();
        Integer len_killed_jobs = killed_jobs.size();

        Integer finished_job_count = len_completed_jobs + len_killed_jobs;
        int all_job_count = finished_job_count + len_started_jobs;
        String preparedStmt = "";
        if (((float) finished_job_count / (float) all_job_count) > 0.9) {
            String sql = "UPDATE processing_state SET state='%s',running_job_count=%d,completed_job_count=%d,killed_job_count=%d,last_update=NOW() WHERE (site_id=%d) AND (run_id=%d)";
            preparedStmt = String.format(sql, "COMPLETED", len_started_jobs, len_completed_jobs, len_killed_jobs, siteId, run_id);
            logger.trace(preparedStmt);
            if (this.conn.query(preparedStmt)) {
                String logMsg = "Processing State of site %d in Run %d marked as COMPLETE";
                logger.info(String.format(logMsg, siteId, run_id));

            }
            else{
                logger.error(String.format("Unable to update processing state. \nArgs - siteId: %d\t, runId: %d",siteId,run_id));
            }
            manageRunState(run_id);
        } else {
            String sql = "UPDATE processing_state SET running_job_count=%d,completed_job_count=%d,killed_job_count=%d,last_update=NOW() WHERE (site_id=%d) AND (run_id=%d)";
            preparedStmt = String.format(sql, len_started_jobs, len_completed_jobs, len_killed_jobs, siteId, run_id);
            logger.trace(preparedStmt);
            if (this.conn.query(preparedStmt)) {
                String logMsg = "Job counts in processing state of site %d in Run %d updated";
                logger.debug(String.format(logMsg, siteId, run_id));

            }
            else{
                logger.error(String.format("Unable to update processing state. \nArgs - siteId: %d\t, runId: %d",siteId,run_id));
            }
        }


    }

    public ArrayList<String> getAllJobsByStateAndSite(int run_id, String state, Integer siteId) {
        String sql = "SELECT job_id FROM jobs WHERE (job_state='%s') AND (run_id=%d) AND (site_id=%d)";
        String preparedStmt = String.format(sql, state, run_id, siteId);
        logger.trace(preparedStmt);
        ArrayList<String> job_id_list = new ArrayList<String>();
        if (this.conn.query(preparedStmt)) {
            while (conn.moveNext()) {
                job_id_list.add(this.conn.gets(1));
            }
        }
        else{
            logger.error(String.format("Unable to get jobs by state and site. \nArgs - siteId: %d\t, runId: %d\t, state: %s",siteId,run_id,state));
        }
        return job_id_list;
    }


    public void updateJobState(Integer jobId, String state, Integer siteId, int run_id) {
        String sql = "UPDATE jobs SET job_state='%s'" +
                ",last_update=NOW() WHERE job_id='%s'";
        String preparedStmt = String.format(sql, state, jobId);
        logger.trace(preparedStmt);
        if (this.conn.query(preparedStmt)) {
            String logMsg = "Job %d marked as %s";
            logger.debug(String.format(logMsg, jobId, state));

        }
        else{
            logger.error(String.format("Unable to update job state. \nArgs - jobId: %d\t siteId: %d\t runId: %d\t state: %s",jobId,siteId,run_id,state));
        }
        updateProcessingState(siteId, run_id);
    }

    public void addJobResults(String entryName, String paramName, String paramValue) {
        Integer jobId = Integer.parseInt(entryName.split("_")[0]);
        String nodeName = entryName.split("_")[1];
        Integer siteId = getSiteIdByJobId(jobId);
        Integer nodeId = getNodeIdByNodeName(siteId, nodeName);
        if (nodeId == -1) {
            // If node does not exist, create Node
            nodeId = addNode(siteId, nodeName);
        }

        if ((paramName.equals("state")) && (paramValue.equals("complete"))) {
            updateJobState(jobId, "COMPLETED", siteId, this.run_id);
        } else {
            String sql = "INSERT INTO parameters (job_id,run_id,node_id,paramName,paramValue,last_update) VALUES(%d, %d, %d, '%s','%s', NOW()) ON DUPLICATE KEY UPDATE job_id=job_id";
            String preparedStmt = String.format(sql, jobId, this.run_id, nodeId, StringEscapeUtils.escapeJava(paramName), StringEscapeUtils.escapeJava(paramValue));
            logger.trace(preparedStmt);
            if (this.conn.query(preparedStmt)) {
                String logMsg = "New parameter added. Job ID: %d\t Node ID: %d\t Site ID: %d\t Name: %s\t Value: %s";
                logger.info(String.format(logMsg, jobId, nodeId, siteId, paramName, paramValue));

            }
            else{
                logger.error(String.format("Unable to add job parameters to the database. \nArgs - entryName: %s\t, paramName: %d\t siteId: %d\t nodeId: %d",entryName,paramName,siteId,nodeId));
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

    }

}
