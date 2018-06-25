package connector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by evgeniyh on 6/24/18.
 */

public class DBManager {
    private static final Logger logger = LogManager.getLogger(DBManager.class);
    protected final ConnectionManager connection;

    public DBManager(ManagerConfig config) throws SQLException, ClassNotFoundException {
         connection = new ConnectionManager(config);
    }

    public void createTableIfMissing(DatabaseMetaData dbm, String tableName, String createTableQuery) throws SQLException {
        logger.info("Verifying table with name - " + tableName + " exists");
        ResultSet tables = dbm.getTables(null, null, tableName, null);

        if (tables.next()) {
            logger.info("The table - " + tableName + " already exists - skipping creation");
        } else {
            logger.info("The table - " + tableName + " doesn't exists - creating it now");
            PreparedStatement ps = connection.prepareStatement(createTableQuery);
            executeUpdateStatement(ps, false);
            logger.info("Table - " + tableName + " was created successfully");
        }
    }

    public ResultSet readAllTable(String tableName) throws SQLException {
        String selectAll = String.format("SELECT * FROM %s;", tableName);
        PreparedStatement ps = connection.prepareStatement(selectAll);

        logger.info("Executing query - " + ps);
        return ps.executeQuery();
    }

    public void deleteTable(String tableName) throws SQLException {
        logger.info("Deleting table - " + tableName);

        String deleteTableQuery =  String.format("DROP TABLE %s ;", tableName);
        PreparedStatement ps = connection.prepareStatement(deleteTableQuery);
        executeUpdateStatement(ps, false);
    }

    public PreparedStatement prepareStatement(String query) throws SQLException {
        return connection.prepareStatement(query);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        logger.info("Getting metadata from the data base");
        return connection.getMetaData();
    }

    protected void executeUpdateStatement(PreparedStatement ps, boolean silent) throws SQLException {
        if (!silent) {
            logger.info("Executing update statement - " + ps);
        }
        if (ps == null) {
            throw new NullPointerException("Failed to create statement");
        }
        int affectedRows = ps.executeUpdate();

        if (!silent) {
            logger.info(String.format("The update statement completed successfully and affected %d rows", affectedRows));
        }
    }
}
