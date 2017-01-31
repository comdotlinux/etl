/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linux.enhydrator;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Memory;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author guru.a.kulkarni
 */
public class FirstAttemptTest {
    
    private static final String SRC_TEST_RESOURCES = "src/test/resources";
    private static final Source SRC = new CSVFileSource(SRC_TEST_RESOURCES + "/com/linux/enhydrator/FL_insurance_sample.csv", ",", "utf-8", true);
    private Pump.Engine engine;
    
    @Before
    public void setUp(){
        this.engine = new Pump.Engine().from(SRC);
    }
    
     @Test
     public void readCsvToMemory() {
        Pump pump = engine.to(new VirtualSinkSource())
//                .to(new LogSink())
                .to(new CSVFileSink(null, "target/tmp.csv", ",", true, false))
                .build();
         Memory memory = pump.start();
         assertThat(memory, is(notNullValue()));
         assertThat(memory.areErrorsOccured(), is(false));
     }
}
