/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linux.enhydrator;

import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
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
public class InputDatabaseTest {

    private static UnmanagedConnectionProvider connectionProvider;
    
    @BeforeClass
    public static void setUpClass() {
        connectionProvider = new UnmanagedConnectionProvider("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/dbname", "etltest", "o7Y67SE0204VWCB");
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
