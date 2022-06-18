package org.apache.bookkeeper.tools.cli.commands.bookies;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.*;

//Command to ledger data recovery for failed bookie.
@RunWith(Parameterized.class)
public class RecoverCommandTest {

    //constant fields
    private static final String validBookie = "127.0.0.1:8000";
    private static final String invalidBookie = "ReadOnly"; //parsing error on BookieId.parse(): ![a-zA-Z0-9:-_.\\-]+ OR readonly
    private static final long defaultLedger = -1L; //default ledger

    private RecoverCommand rc;

    //params
    private final ServerConfiguration conf;
    private final RecoverCommand.RecoverFlags cmdFlags;
    private final String bookieAddress;
    private final long ledger;
    private final boolean query;
    private final boolean skipOpenLedgers;

    private Class<?> expectedException;
    private boolean expected;

    @BeforeClass
    public static void setUpMocks(){
        mockServer();
        mockClient();
        mockAdmin();
    }

    private static void mockAdmin() {
        Mockito.mockConstruction(BookKeeperAdmin.class, (ba, context) ->
                Mockito.doAnswer(i -> new ClientConfiguration()).when(ba).getConf());
    }

    private static void mockClient() {
        Mockito.mockConstruction(ClientConfiguration.class, (cc, context) ->
                Mockito.doReturn("zk1:8000").when(cc).getMetadataServiceUri());
    }

    private static void mockServer() {
        Mockito.mockConstruction(ServerConfiguration.class, (sc, context) ->
                Mockito.doReturn("zk1:8000").when(sc).getMetadataServiceUri());
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        Object[][] params = {
                //exception
                {new ServerConfiguration(), null, null, defaultLedger, true, false}, //unchecked
                {null, new RecoverCommand.RecoverFlags(), null, defaultLedger, false, false}, //unchecked
                //false
                {new ServerConfiguration(), new RecoverCommand.RecoverFlags(), invalidBookie, defaultLedger, false, false}, //parsing error
                //true
                {new ServerConfiguration(), new RecoverCommand.RecoverFlags(), validBookie, 1, false, true}, //bkRecoveryLedger
                {new ServerConfiguration(), new RecoverCommand.RecoverFlags(), validBookie, defaultLedger, false, false}, //bkRecovery
                {new ServerConfiguration(), new RecoverCommand.RecoverFlags(), validBookie, -12, true, false}, //bkQuery
        };

        return Arrays.asList(params);
    }

    public RecoverCommandTest(ServerConfiguration param1, RecoverCommand.RecoverFlags param2, String param3,
                              long param4, boolean param5, boolean param6){
        this.conf = param1;
        this.cmdFlags = param2;

        this.bookieAddress = param3;
        this.ledger = param4;
        this.query = param5;
        this.skipOpenLedgers = param6;
    }

    @Before
    public void setUp() {
        this.rc = new RecoverCommand();

        if(cmdFlags != null){
            setUpFlags();
        }

        oracle();
    }

    private void setUpFlags() {
        cmdFlags.force(true); //avoids user interaction

        if(bookieAddress != null){
            cmdFlags.bookieAddress(bookieAddress);
        }

        if(ledger != 0){
            cmdFlags.ledger(ledger);
        }

        if(query){
            cmdFlags.query(true);
        }

        if(skipOpenLedgers){
            cmdFlags.skipOpenLedgers(true);
        }
    }

    private void oracle() {
        if(conf == null || cmdFlags == null || bookieAddress == null){
            this.expectedException = UncheckedExecutionException.class;
        }

        this.expected = !Objects.equals(bookieAddress, invalidBookie);
    }

    @Test
    public void applyTest(){
        try{
            boolean res = rc.apply(conf, cmdFlags);
            Assert.assertEquals(expected, res);

        }catch (Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

}
