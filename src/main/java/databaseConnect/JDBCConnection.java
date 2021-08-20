package databaseConnect;

import org.jooq.impl.DSL;
import utils.Log;
import java.sql.*;

public class JDBCConnection {
    private static final String host = "localhost";
    private static final String DBName = "sakila";
    private static final String url = "jdbc:mysql://" + host + ":3306/" + DBName + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static final String user = "root";
    private static final String password = "myroot";

    private static Connection con = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public static Connection connectToDB() {
        Log.info("Connect to DB " + url + " by user " + user);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
            Log.info("Connection to DB successful!");
        } catch (ClassNotFoundException e) {
            Log.error(e.getMessage());
        } catch (SQLException sqlEx) {
            Log.error("Connection to DB failed!\n" + sqlEx.getMessage());
        }

        return con;
    }

    public static void closeConnection() {
        if (con != null) {
            try {
                con.close();
                Log.info("Connection to DB closed successfully");
            } catch (SQLException se) {
                Log.error("Connection to DB was not closed. Reason:\n" + se.getMessage());
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
                Log.info("Statement closed successfully");
            } catch (SQLException se) {
                Log.error("Statement was not closed. Reason:\n" + se.getMessage());
            }
        }

        if (rs != null) {
            try {
                rs.close();
                Log.info("ResultSet closed successfully");
            } catch (SQLException se) {
                Log.error("ResultSet was not closed. Reason:\n" + se.getMessage());
            }
        }
    }

    public static void createTable(String query) {
        try {
            stmt = connectToDB().prepareStatement(query);
            Log.info("Send request to DB: " + query);
            stmt.executeUpdate(query);
            Log.info("Table was created successfully");
        } catch (SQLException se) {
            Log.error("Table was not created. Reason:\n" + se.getMessage());
        }
    }

    public static void dropTable(String tableName) {
        String query = "DROP TABLE " + tableName;
        try {
            stmt = connectToDB().prepareStatement(query);
            Log.info("Send request to DB: " + query);
            stmt.executeUpdate(query);
            Log.info("Table was deleted successfully");
        } catch (SQLException se) {
            Log.error("Table was not deleted. Reason:\n" + se.getMessage());
        }
    }

    public static ResultSet selectFromTable(String query) {
        try {
            stmt = connectToDB().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            Log.info("Send request to DB: " + query);
            rs = stmt.executeQuery(query);
            rs.next();
            addSQLRequestResultsToLog(query);
        } catch (SQLException se) {
            Log.error(se.getMessage());
        }
        return rs;
    }

    private static void addSQLRequestResultsToLog(String query) {
        StringBuilder builder = new StringBuilder();
        DSL.using(connectToDB()).fetchStream(query)
                .forEach(r -> builder.append(r.format()));
        Log.info("Request results:\n" + builder);
    }

    public static void insertIntoTable(String query) {
        try {
            stmt = connectToDB().createStatement();
            Log.info("Send request to DB: " + query);
            stmt.executeUpdate(query);
            Log.info("New data was inserted into table successfully");
        } catch (SQLException se) {
            Log.error("New data was not inserted into table. Reason:\n" + se.getMessage());
        }
    }

    public static void updateInTable(String query) {
        try {
            stmt = connectToDB().createStatement();
            Log.info("Send request to DB: " + query);
            stmt.executeUpdate(query);
            Log.info("Data in table was updated successfully");
        } catch (SQLException se) {
            Log.error("Data in table was not updated. Reason:\n" + se.getMessage());
        }
    }

    public static void deleteFromTable(String query) {
        try {
            Log.info("Send request to DB: " + query);
            stmt = connectToDB().createStatement();
            stmt.executeUpdate(query);
            Log.info("Data from table was deleted successfully");
        } catch (SQLException se) {
            Log.error("Data from table was not deleted. Reason:\n" + se.getMessage());
        }
    }

}

