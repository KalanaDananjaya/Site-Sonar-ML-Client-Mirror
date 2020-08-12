import lia.Monitor.Store.Fast.DB;

public class TestDbConnection {

    public static void main(String[] args) {
        DB conn = new DB();
        // Test database connectivity
        String sql = "SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1";
        if (conn.query(sql)) {
            System.err.println("Database Connection Successful");
            String run_id = conn.gets(1);
            String backend = conn.getBackend().getBackendName();
            System.err.println("Established connection with " + backend);
            System.err.println("Latest run id: " + run_id);
        } else {
            System.err.println("Unable to connect to database");
        }
    }
}
