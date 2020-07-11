import lia.Monitor.Store.Fast.DB;

public class DatabaseConnection {
    DB conn;

    /**
     * Connect to the database
     */
    public DatabaseConnection() {
         this.conn = new DB();
    }


    public Integer getSiteIdByJobId(Integer jobId){
        System.err.println("Get siteid called");

        Integer siteId = -1;
        String sql = "SELECT site_id FROM jobs WHERE job_id = %d";
        String preparedStmt = String.format(sql,jobId);
        if (this.conn.query(preparedStmt)){
            if (conn.moveNext()){
                siteId =  conn.geti("site_id");
            }
        }
        System.err.println("get site succesfully");
        return siteId;
    }


    public Integer addNode(Integer siteId, String nodeName){
        System.err.println("add node called");
        String sql = "INSERT INTO nodes (site_id,node_name) VALUES (%d, '%s')";
        String preparedStmt = String.format(sql,siteId,nodeName);
        Integer nodeId = -1;
       
        if (this.conn.query(preparedStmt)){
            if (this.conn.query("SELECT LAST_INSERT_ID()")){
                if (conn.moveNext()){
                    nodeId =  Integer.parseInt(conn.gets(1));
                }
            }
        }
        System.err.println("Cadd node succesfully");
        return nodeId;
    }


    public Integer getNodeIdByNodeName(Integer siteId, String nodeName){
        System.err.println("Get node by name called");
        String sql = "SELECT node_id FROM nodes WHERE site_id=%d and node_name='%s'";
        String preparedStmt = String.format(sql,siteId,nodeName);
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
        System.err.println("get node by id established succesfully");

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
        System.err.println(nodeId+","+siteId);

        String sql = "INSERT INTO parameters (job_id,node_id,paramName,paramValue,last_update) VALUES(%d,%d,'%s','%s',NOW())";
        String preparedStmt = String.format(sql,jobId,nodeId,paramName,paramValue);
        System.err.println("Updating job"+ Integer.toString(jobId) + "with " + Integer.toString(nodeId) + " " + paramName + ":" + paramValue);
        this.conn.query(preparedStmt);

        System.err.println("add job result established succesfully");

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

    }

}
