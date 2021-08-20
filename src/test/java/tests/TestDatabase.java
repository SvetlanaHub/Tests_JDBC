package tests;

import databaseConnect.JDBCConnection;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Checking the connection to the sakila database and sending requests")

public class TestDatabase extends TestsSetup {
    @Test
    @Order(1)
    @DisplayName("Checking the creation of the humans table in the sakila DB")
    public void testCreateTableStudents() {

        String query = "CREATE TABLE humans ("
                + "ID int(6) NOT NULL,"
                + "FIRST_NAME VARCHAR(45) NOT NULL,"
                + "LAST_NAME VARCHAR(45) NOT NULL,"
                + "TOWN VARCHAR(45),"
                + "PRIMARY KEY (id))";
        JDBCConnection.createTable(query);

    }

    @Test
    @Order(2)
    @DisplayName("Insert data in table")
    public void testInsertRequest() {

        String query = "INSERT INTO humans (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (1, 'David', 'Brown', 'New York')";
        JDBCConnection.insertIntoTable(query);
        String selectQuery = "SELECT * FROM humans";
        ResultSet rs = JDBCConnection.selectFromTable(selectQuery);
        Assertions.assertAll("Should return inserted data",
                () -> assertEquals("1", rs.getString("ID")),
                () -> assertEquals("David", rs.getString("FIRST_NAME")),
                () -> assertEquals("Brown", rs.getString("LAST_NAME")),
                () -> assertEquals("New York", rs.getString("TOWN")));

    }

    @Test
    @Order(3)
    @DisplayName("Update data in a table")
        public void testUpdateRequest() throws SQLException {

        String query = "UPDATE humans SET TOWN = 'PARIS' WHERE ID=1";
        JDBCConnection.updateInTable(query);
        String selectQuery = "SELECT TOWN FROM humans WHERE ID=1";
        ResultSet rs = JDBCConnection.selectFromTable(selectQuery);
        String expectedTown = "PARIS";
        String actualTown = rs.getString("TOWN");
        assertEquals(expectedTown, actualTown, "Actual town is '" + actualTown + "'. Expected - '" + expectedTown + "'.");

    }

    @Test
    @Order(4)
    @DisplayName("Sending a simple SELECT query. Checking the country")
    public void testSelectRequest_checkAddress() throws SQLException {

        String query = "SELECT * FROM country WHERE country_id=6";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String expectedCountry = "Argentina";
        String actualCountry = rs.getString("country");
        assertEquals(expectedCountry, actualCountry, "Actual address is '" + actualCountry + "'. Expected - '" + expectedCountry + "'.");

    }

    @Test
    @Order(5)
    @DisplayName("Sending a SELECT JOIN query. Checking the belonging of the city to the country")
    public void testSelectWithJoinRequest_CheckCityAndCountry() throws SQLException {

        String query = "SELECT ct.city, cntr.country FROM city ct LEFT JOIN country cntr ON ct.country_id=cntr.country_id WHERE city='Dallas'";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String expectedCountry = "United States";
        String actualCountry = rs.getString("country");
        assertEquals(expectedCountry, actualCountry, "Actual country is '" + actualCountry + "'. Expected - '" + expectedCountry + "'.");

    }

    @Test
    @Order(6)
    @DisplayName("Deleting data from the table")
    public void testDeleteRequest() {

        String query = "DELETE FROM humans WHERE ID=1";
        JDBCConnection.deleteFromTable(query);

    }

    @Test
    @Order(7)
    @DisplayName("Deleting the Table from the Database")
    public void test_dropTable() {

        JDBCConnection.dropTable("humans");

    }

}