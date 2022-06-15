package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.*;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.Assert.fail;

/*EnsemblePlacementPolicy encapsulates the algorithm that bookkeeper client uses to select a number of bookies from the
    cluster as an ensemble for storing entries.*/

@RunWith(Enclosed.class)
public class ZoneawareEnsemblePlacementPolicyImplTest {

    /*
    * Returns AdherenceLevel if the Ensemble is strictly/softly/fails adhering to placement policy.
    *
    * If bookies in the write-set are from 'desiredNumOfZones' then it is considered as MEETS_STRICT.
    * If they are from 'minNumOfZones' then it is considered as MEETS_SOFT.
    * Otherwise, considered as FAIL.
    * */
    @RunWith(Parameterized.class)
    public static class IsEnsembleAdheringToPlacementPolicyTest{

        //constant fields
        private final int desiredNumOfZones = 2;
        private final int minNumOfZones = 1;

        private ZoneawareEnsemblePlacementPolicyImpl policy;
        private final String[] zones;

        private final List<BookieId> ensembleList;
        private final int writeQuorumSize;
        private final int ackQuorumSize;

        private EnsemblePlacementPolicy.PlacementPolicyAdherence expected;
        private Class<?> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {null, 1, 2, null},
                    {new ArrayList<>(), 2, 1, null},
                    {new ArrayList<>(), 0, 0, null},
                    {Collections.singletonList(BookieId.parse("127.0.0.1:8000")), 1, 1, new String[]{"/region-a/rack-1"}},
                    {Arrays.asList(BookieId.parse("127.0.0.1:8000"), BookieId.parse("127.0.0.2:8000")), 2, 1,
                            new String[]{"/region-a/rack-1", "/region-a/rack-2"}},
                    {Arrays.asList(BookieId.parse("127.0.0.1:8000"), BookieId.parse("127.0.0.2:8000")), 2, 1,
                            new String[]{"/region-a/rack-1", "/region-b/rack-1"}},
                    {Collections.singletonList(BookieId.parse("127.0.0.1:8000")), -2, -1, new String[]{"/region-a/rack-1"}},
                    {Collections.singletonList(BookieId.parse("127.0.0.1:8000")), 2, -2, new String[]{"/region-a/rack-1"}},
            };

            return Arrays.asList(params);
        }

        public IsEnsembleAdheringToPlacementPolicyTest(List<BookieId> param1, int param2, int param3, String[] param4) {
            this.ensembleList = param1;
            this.writeQuorumSize = param2;
            this.ackQuorumSize = param3;

            this.zones = param4;
        }

        @Before
        public void setUp() throws UnknownHostException {
            this.policy = new ZoneawareEnsemblePlacementPolicyImpl();

            List<BookieSocketAddress> addresses = new ArrayList<>();
            if(ensembleList != null && !ensembleList.isEmpty()){
                for(BookieId bid: ensembleList){
                    addresses.add(new BookieSocketAddress(bid.toString()));
                }
            }

            int i = 0;
            if(!addresses.isEmpty()){
                for(BookieSocketAddress address: addresses){
                    StaticDNSResolver.addNodeToRack(address.getHostName(), zones[i]);
                    i++;
                }
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.setDesiredNumZonesPerWriteQuorum(desiredNumOfZones);
            conf.setMinNumZonesPerWriteQuorum(minNumOfZones);
            conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

            policy.initialize(conf, Optional.empty(), null, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            oracle();
        }

        private void oracle(){
            if(ensembleList == null){
                this.expectedException = NullPointerException.class;
                return;
            }

            if(writeQuorumSize == 0){
                this.expectedException = ArithmeticException.class;
                return;
            }

            if(ensembleList.size() % writeQuorumSize != 0){
                this.expected = EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL;
                return;
            }

            if(ensembleList.size() == minNumOfZones){
                this.expected = EnsemblePlacementPolicy.PlacementPolicyAdherence.MEETS_SOFT;

            }else if(ensembleList.size() == desiredNumOfZones){
                String[] tokens1 = zones[0].split("/");
                String[] tokens2 = zones[1].split("/");

                if(tokens1[1].equals(tokens2[1])){
                    this.expected = EnsemblePlacementPolicy.PlacementPolicyAdherence.MEETS_SOFT;
                }else{
                    this.expected = EnsemblePlacementPolicy.PlacementPolicyAdherence.MEETS_STRICT;
                }
            }else{
                this.expected = EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL;
            }
        }

        @Test
        public void isEnsembleAdheringToPlacementPolicyTest(){

            try{
                EnsemblePlacementPolicy.PlacementPolicyAdherence res =
                        policy.isEnsembleAdheringToPlacementPolicy(ensembleList, writeQuorumSize, ackQuorumSize);

                Assert.assertEquals(expected, res);

            }catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown(){
            policy.uninitalize();
        }
    }

    /*
    * 1. If ensembleSize is more than the number of available nodes, BKException.BKNotEnoughBookiesException is thrown.
    *
    * 2. The size of the returned bookie list should be equal to the provided ensembleSize.
    *
    * 3. If 'enforceMinNumRacksPerWriteQuorum' config is enabled then the bookies belonging to default fault zone
    * (rack) will be excluded while selecting bookies.
    */
    @RunWith(Parameterized.class)
    public static class NewEnsembleTest {

        //constant fields
        private final List<BookieId> availableSet = Arrays.asList(BookieId.parse("127.0.0.1:8000"),
                                                                  BookieId.parse("127.0.0.2:8000"));
        private final String[] zones = {"/region-a/rack-1", "/region-b/rack-1"};

        private ZoneawareEnsemblePlacementPolicyImpl policy;

        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final Map<String,byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;

        private int expectedSize;
        private Class<?> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    //tests on params
                    {0, 1, 2, null, new HashSet<>()},
                    {-1, -2, -2, new LinkedHashMap<String, byte[]>(), new HashSet<>()}, //negative
                    {2, 1, 1, new LinkedHashMap<String, byte[]>(), null}, //excluded set null
                    {2, -1, -1, null, null},
                    {1, 0, 2, new LinkedHashMap<String, byte[]>(), new HashSet<>()}, //wq 0

                    //tests on functionality: valid params + correct behavior
                    {2, 1, 0, null, new HashSet<>()},
                    {1, 1, 1, Collections.singletonMap("property", "bytes".getBytes(StandardCharsets.UTF_8)),
                            new HashSet<>(Collections.singletonList(BookieId.parse("127.0.0.1:8000")))},
                    {2, 1, 1, null, new HashSet<>(Collections.singletonList(BookieId.parse("127.0.0.1:8000")))}, // not enough bookies
            };

            return Arrays.asList(params);
        }

        public NewEnsembleTest(int param1, int param2, int param3, Map<String,byte[]> param4, HashSet<BookieId> param5) {
            this.ensembleSize = param1;
            this.writeQuorumSize = param2;
            this.ackQuorumSize = param3;
            this.customMetadata = param4;
            this.excludeBookies = param5;
        }

        @Before
        public void setUp() throws UnknownHostException {
            this.policy = new ZoneawareEnsemblePlacementPolicyImpl();

            List<BookieSocketAddress> addresses = new ArrayList<>();
            for(BookieId bid: availableSet){
                addresses.add(new BookieSocketAddress(bid.getId()));
            }

            int i = 0;
            if(!addresses.isEmpty()){
                for(BookieSocketAddress address: addresses){
                    StaticDNSResolver.addNodeToRack(address.getHostName(), zones[i]);
                    i++;
                }
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.setDesiredNumZonesPerWriteQuorum(writeQuorumSize); //avoids errors of misconfiguration on write quorum
            conf.setMinNumZonesPerWriteQuorum(writeQuorumSize-1); // --
            conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

            policy.initialize(conf, Optional.empty(), null, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            policy.onClusterChanged(new HashSet<>(availableSet), new HashSet<>());

            oracle();
        }

        private void oracle() {
            if((this.expectedException = invalidArgsException()) != null){
                return;
            }

            int available = availableSet.size();
            if(!excludeBookies.isEmpty()){
                available -= excludedInAvailable();
            }

            if (available < ensembleSize) {
                this.expectedException = BKException.BKNotEnoughBookiesException.class;

            } else {
                this.expectedSize = ensembleSize;
            }
        }

        private Class<?> invalidArgsException() {
            Class<?> exception = null;

            if(writeQuorumSize == 0){
                exception = ArithmeticException.class;

            }else if (ensembleSize < 0 || ensembleSize % writeQuorumSize != 0) {
                exception = IllegalArgumentException.class;

            }else if(excludeBookies == null){
                exception = NullPointerException.class;
            }

            return exception;
        }

        private int excludedInAvailable() {
            int tot = 0;
            for(BookieId exBid: excludeBookies){
                for(BookieId avBid: availableSet){
                    if(avBid.toString().contains(exBid.toString())){
                        tot++;
                    }
                }
            }

            return tot;
        }

        @Test
        public void newEnsembleTest() {
            Assume.assumeTrue(expectedException == null);
            try {
                EnsemblePlacementPolicy.PlacementResult<List<BookieId>> res =
                        policy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);

                Assert.assertNotNull(res);
                Assert.assertEquals(expectedSize, res.getResult().size());

            } catch (Exception e) {
                fail("Threw exception: " + e.getClass());
            }
        }

        @Test
        public void newEnsembleExceptionTest() {
            Assume.assumeTrue(expectedException != null);

            try {
                policy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                fail("Expected: " + expectedException);

            } catch (Exception e) {
                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown() {
            policy.uninitalize();
        }
    }

    /*
    * Returns true if the bookies that have acknowledged a write
    * adhere to the minimum fault domains as defined in the placement policy in use.
    * */
    @RunWith(Parameterized.class)
    public static class AreAckedBookiesAdheringToPlacementPolicyTest{

        //constant fields
        private final String[] zones = {"/region-a/rack-1", "/region-b/rack-1", "/region-c/rack-1"};

        private ZoneawareEnsemblePlacementPolicyImpl policy;

        private final Set<BookieId> ackedBookies;
        private final int writeQuorumSize;
        private final int ackQuorumSize;

        private boolean expected;
        private Class<?> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getTestParameters() {
            Object[][] params = {
                    {null, 1, 2}, //exception: null

                    {new HashSet<>(), 0, 0}, //no bookies, 0 acked -> set = ack -> adheres
                    {new HashSet<>(), 2, 1}, //no bookies, 1 acked -> set < ack -> doesn't adhere
                    {new HashSet<>(), 2, -1}, //neg ack -> doesn't adhere
                    {new HashSet<>(Collections.singletonList(BookieId.parse("127.0.0.1:8000"))), 1, 0}, //1 bookie, 0 acked -> set > ack -> adheres
                    {new HashSet<>(Arrays.asList(
                            BookieId.parse("127.0.0.1:8000"),
                            BookieId.parse("127.0.0.2:8000"),
                            BookieId.parse("127.0.0.3:8000"),
                            BookieId.parse("127.0.0.4:8000"))), -1, 4}, //4 bookies, 4 acked -> set = ack -> adheres
                    {new HashSet<>(Arrays.asList(
                            BookieId.parse("127.0.0.1:8000"),
                            BookieId.parse("127.0.0.2:8000"),
                            BookieId.parse("127.0.0.3:8000"),
                            BookieId.parse("127.0.0.4:8000"))), 1, 5} //4 bookies, 5 acked -> set < ack -> doesn't adhere
            };

            return Arrays.asList(params);
        }

        public AreAckedBookiesAdheringToPlacementPolicyTest(Set<BookieId> param1, int param2, int param3) {
            this.ackedBookies = param1;
            this.writeQuorumSize = param2;
            this.ackQuorumSize = param3;
        }

        @Before
        public void setUp() throws UnknownHostException {
            this.policy = new ZoneawareEnsemblePlacementPolicyImpl();

            List<BookieSocketAddress> addresses = new ArrayList<>();
            if(ackedBookies != null && !ackedBookies.isEmpty()){
                for(BookieId bid: ackedBookies){
                    addresses.add(new BookieSocketAddress(bid.toString()));
                }
            }

            int i = 0;
            if(!addresses.isEmpty()){
                for(BookieSocketAddress address: addresses){
                    StaticDNSResolver.addNodeToRack(address.getHostName(), zones[i]);
                    if(++i >= zones.length){
                        i = 0; //round-robin
                    }
                }
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

            policy.initialize(conf, Optional.empty(), null, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            oracle();
        }

        private void oracle(){
            if(ackedBookies == null){
                this.expectedException = NullPointerException.class;
                return;
            }

            this.expected = ackQuorumSize >= 0 && ackQuorumSize <= ackedBookies.size();

        }

        @Test
        public void areAckedBookiesAdheringToPlacementPolicyTest(){

            try{
                boolean areAcked =
                        policy.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, writeQuorumSize, ackQuorumSize);

                Assert.assertEquals(expected, areAcked);

            }catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
            }
        }

        @After
        public void tearDown(){
            policy.uninitalize();
        }
    }

}
