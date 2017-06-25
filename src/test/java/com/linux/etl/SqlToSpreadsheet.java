package com.linux.etl;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeNameMapper;
import com.airhacks.enhydrator.transform.Memory;

/**
 * Convert Database Query output to a comma separated file.
 * This is the main class to run.
 * @author comdotlinux
 */
public class SqlToSpreadsheet {
    private static final String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    
    public static void main(String[] args) {
        
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
                .to(new CSVFileSink("*", "target/output.csv", ",", true, false))
                .to(new LogSink())
                .build(); // Build Pump
        
        Memory memory = pump.start(); // Start the pump, get the memory
    }
}
