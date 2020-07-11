import lia.Monitor.Store.Fast.DB;

public class DatabaseConnection {
    DB conn;
    int run_id;

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
        String preparedStmt = String.format(sql,this.run_id,siteId,nodeName);
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

    public void addJobResults(String entryName, String paramName, String paramValue) {
        Integer jobId = Integer.parseInt(entryName.split("_")[0]);
        String nodeName = entryName.split("_")[1];
        Integer siteId = getSiteIdByJobId(jobId);
        Integer nodeId = getNodeIdByNodeName(siteId, nodeName);
        if (nodeId == -1 ){
            // If node does not exist, create Node
            nodeId = addNode(siteId, nodeName);
        }

        String sql = "INSERT INTO parameters (job_id,run_id,node_id,paramName,paramValue,last_update) VALUES(%d, %d, %d, '%s','%s', NOW())";
        String preparedStmt = String.format(sql,jobId,this.run_id,nodeId,paramName,paramValue);
        if (this.conn.query(preparedStmt,true)){
            System.err.println("New parameter added. Job ID: " + jobId + " Name: " + paramName + " Value:" + paramValue
                    + " Node Id: " + nodeId + " Site ID:" + siteId + " Run ID:" + run_id);
        };

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

    }

}
