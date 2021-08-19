package tests;

import databaseConnect.JDBCConnection;
import org.junit.jupiter.api.*;
import utils.Log;

public class TestsSetup {

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        Log.info("------- Started test: " + testInfo.getDisplayName() + " -------");
        Assertions.assertNotNull(JDBCConnection.connectToDB());
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {
        JDBCConnection.closeConnection();
        Log.info("------- Finished test: " + testInfo.getDisplayName() + " -------");
    }

}
