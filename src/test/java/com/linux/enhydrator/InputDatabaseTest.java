package com.linux.enhydrator;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.Pump.Engine;
import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeNameMapper;
import com.airhacks.enhydrator.transform.Memory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 * Unit test to use enhydrator for retrieving data from database.
 *
 * @author guru.a.kulkarni
 */
public class InputDatabaseTest {

    private static UnmanagedConnectionProvider connectionProvider;
    private static final String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String CSV_OUTPUT = "target/IndiaCapitals.csv";
    private static final SortedMap<String, String> STATE_TO_CAPITAL_CITY = new TreeMap<>();

    private Connection connection;

    @BeforeClass
    public static void setUpClass() {
        try {
            Files.deleteIfExists(Paths.get(CSV_OUTPUT));
        } catch (IOException ex) {
            Logger.getLogger(InputDatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        connectionProvider = new UnmanagedConnectionProvider(DERBY_EMBEDDED_DRIVER, "jdbc:derby:memory:testDB;create=true", "", "");

        STATE_TO_CAPITAL_CITY.put("Mahashtra", "Mumbai");
        STATE_TO_CAPITAL_CITY.put("Tamil Nadu", "Chennai");
        STATE_TO_CAPITAL_CITY.put("Punjab", "Chandigarh");
        STATE_TO_CAPITAL_CITY.put("West Bengal", "Kolkata");
        STATE_TO_CAPITAL_CITY.put("Madhya Pradesh", "Bhopal");
        STATE_TO_CAPITAL_CITY.put("Karnataka", "Bengaluru");
        STATE_TO_CAPITAL_CITY.put("Gujrat", "Ahmedabad");
        STATE_TO_CAPITAL_CITY.put("Goa", "Panji");
        STATE_TO_CAPITAL_CITY.put("Andra Pradesh", "Hyderabad");

    }

    @Before
    public void setUp() throws SQLException {
        connectionProvider.connect();
        this.connection = connectionProvider.get();
        PreparedStatement ps = this.connection.prepareStatement("CREATE TABLE INDIA_CAPITALS ("
                + "id INT PRIMARY KEY NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "state VARCHAR(150),"
                + "capital_city VARCHAR(150))");

        ps.execute();
        String insertQuery = "INSERT INTO INDIA_CAPITALS(state, capital_city) VALUES ('%s', '%s')";
        for (Map.Entry<String, String> entry : STATE_TO_CAPITAL_CITY.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String insert = String.format(insertQuery, key, value);
            this.connection.prepareStatement(insert).execute();
            this.connection.commit();
        }

    }

    @Ignore
    @Test
    public void readFromDatabase() throws SQLException {
        JDBCSource jdbcSource = new JDBCSource.Configuration()
                .driver(DERBY_EMBEDDED_DRIVER)
                .url("jdbc:derby:memory:testDB")
                .user("")
                .password("")
                .newSource();

        // Create Engine
        Engine engine = new Pump.Engine()
                .from(jdbcSource) // set source
                .sqlQuery("SELECT * FROM INDIA_CAPITALS") // query to retrieve data
                .with("id", (id) -> id instanceof Integer ? (Integer) id : id) // column 1
                .with("state", (state) -> String.valueOf(state)) // column 2
                .with("capital_city", (city) -> String.valueOf(city)); // column 3
        Pump pump = engine.to(new LogSink()).build(); // Build Pump
        Memory memory = pump.start(); // Start the pump, get the memory

        assertThat(memory.areErrorsOccured(), is(false));
        assertThat(memory.getProcessedRowCount(), is((long) STATE_TO_CAPITAL_CITY.size()));
    }

    @Test
    public void readFromDatabaseAndWriteToCsv() {
        JDBCSource jdbcSource = new JDBCSource.Configuration()
                .driver(DERBY_EMBEDDED_DRIVER)
                .url("jdbc:derby:memory:testDB")
                .user("")
                .password("")
                .newSource();

        
        // Create Engine
        Pump pump = new Pump.Engine()
                .from(jdbcSource) // set source
                .sqlQuery("SELECT * FROM INDIA_CAPITALS") // query to retrieve data
                .continueOnError()
                .startWith(new DatatypeNameMapper().addMapping("id", Datatype.INTEGER))
                .to(new CSVFileSink("*", CSV_OUTPUT, ",", true, false))
                .to(new LogSink())
                .build(); // Build Pump
        
        Memory memory = pump.start(); // Start the pump, get the memory

        assertThat(memory.areErrorsOccured(), is(false));
        assertThat(memory.getProcessedRowCount(), is((long) STATE_TO_CAPITAL_CITY.size()));
    }

    @After
    public void tearDown() throws SQLException {
        this.connection.prepareStatement("DROP TABLE INDIA_CAPITALS").execute();
        this.connection.commit();
        this.connection.close();
    }

    @AfterClass
    public static void tearDownClass() {
        connectionProvider = null;
    }
}
