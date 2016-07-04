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
    
     @Test
     public void readCsvToMemory() {
         Source src = new CSVFileSource("./src/main/resources/com/linux/enhydrator/SampleCSVFile_11kb.csv", ",", "ansi", false);
         Pump pump = new Pump.Engine()
                 .from(src)
                 .to(new VirtualSinkSource())
                 .to(new LogSink())
                 .build();
         Memory memory = pump.start();
         assertThat(memory, is(notNullValue()));
     }
}
