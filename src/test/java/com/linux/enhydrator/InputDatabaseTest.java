/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linux.enhydrator;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.Pump.Engine;
import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Memory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit test to use enhydrator for retrieving data from database.
 * @author guru.a.kulkarni
 */
public class InputDatabaseTest {

    private static UnmanagedConnectionProvider connectionProvider;
    private static final String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static JDBCSource jdbcSource;
    
    private static final SortedMap<String, String> STATE_TO_CAPITAL_CITY = new TreeMap<>();

    @BeforeClass
    public static void setUpClass() {
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

    @AfterClass
    public static void tearDownClass() {
        connectionProvider = null;
    }
    private Connection connection;

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
            
        }
        
        this.connection.commit();
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void checkConnection() throws SQLException {
        jdbcSource = new JDBCSource.Configuration()
                .driver(DERBY_EMBEDDED_DRIVER)
                .url("jdbc:derby:memory:testDB")
                .user("")
                .password("")
                .newSource();

        // Create Engine
        Engine engine = new Pump.Engine()
                .from(jdbcSource) // set source
                .sqlQuery("SELECT * FROM INDIA_CAPITALS") // query to retrieve data
                .with("id", (o) -> o instanceof Integer ? (Integer)o : o) // column 1
                .with("state", (o) -> String.valueOf(o)) // column 2
                .with("capital_city", (o) -> String.valueOf(o)); // column 3
        Pump pump = engine.to(new LogSink()).build(); // Build Pump
        Memory memory = pump.start(); // Start the pump, get the memory
        
        assertThat(memory.areErrorsOccured(), is(false));
        assertThat(memory.getProcessedRowCount(), is((long)STATE_TO_CAPITAL_CITY.size()));
    }
}
