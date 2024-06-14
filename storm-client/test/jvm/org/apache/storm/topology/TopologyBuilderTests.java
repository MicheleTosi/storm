package org.apache.storm.topology;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.lambda.SerializableSupplier;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.shade.org.apache.commons.collections.set.CompositeSet;
import org.apache.storm.state.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.storm.topology.BoltEnum.*;
import static org.apache.storm.topology.SpoutEnum.*;
import static org.apache.storm.topology.WorkerHookEnum.*;
import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class TopologyBuilderTests {

    @RunWith(Parameterized.class)
    public static class CreateTopologyTest {

        private final WorkerHookEnum workerHook;
        private final SpoutEnum spout;
        private final BoltEnum bolt;
        private final BoltEnum statefulBolt;
        private final boolean empty;
        private final boolean expException;
        private final boolean nullException;

        public CreateTopologyTest(SpoutEnum spout, BoltEnum bolt, BoltEnum statefulBolt, WorkerHookEnum workerHook, boolean empty, boolean expException, boolean nullException){
            this.spout=spout;
            this.bolt=bolt;
            this.statefulBolt=statefulBolt;
            this.empty=empty;
            this.expException=expException;
            this.nullException=nullException;
            this.workerHook=workerHook;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){
            return Arrays.asList(new Object[][]{
                    {RICH_SPOUT,    BASIC_BOLT,     STATEFUL_BOLT, WORKER_HOOK,    false,    false, false},
                    {RICH_SPOUT,    BASIC_BOLT,     NO_STATEFUL_BOLT, WORKER_HOOK,    false,    false, false},
                    {UNSERIALIZABLE_RICH_SPOUT,    RICH_BOLT,     UNSERIALIZABLE_STATEFUL_BOLT,  UNSERIALIZABLE_WORKER_HOOK,   false,    true, false},
                    {NULL_SPOUT,    UNSERIALIZABLE_BASIC_BOLT,     NO_STATEFUL_BOLT,  NULL_WORKER_HOOK,   false,    true, true},
                    {RICH_SPOUT,    UNSERIALIZABLE_RICH_BOLT,     NULL_BOLT, WORKER_HOOK,     false,    true, true},
                    {UNSERIALIZABLE_RICH_SPOUT,    NULL_BOLT,     STATEFUL_BOLT, UNSERIALIZABLE_WORKER_HOOK,     true,    false, false},
                    {SUPPLIER,    WINDOWED_BOLT,     STATEFUL_WINDOWED_BOLT, NULL_WORKER_HOOK,     false,    true, false},
                    {UNSERIALIZABLE_SUPPLIER,    UNSERIALIZABLE_WINDOWED_BOLT,     UNSERIALIZABLE_STATEFUL_WINDOWED_BOLT, WORKER_HOOK,     false,    true, false},
            });
        }



        @Test
        public void topologyBuilderTest() {
            try {
                String[] bolts = new String[]{"bolt1", "bolt2", "bolt3"};
                ArrayList<String> expBolts=new ArrayList<>();
                ArrayList<String> expSpout=new ArrayList<>();
                Collections.addAll(expBolts, bolts);
                String spout = "spout";
                expSpout.add(spout);
                Utils utils=new Utils();
                int boltsCount=0;
                TopologyBuilder topologyBuilder = new TopologyBuilder();
                if (this.empty) {
                    StormTopology stormTopology = topologyBuilder.createTopology();
                    assertEquals(0, stormTopology.get_bolts_size());
                } else {
                    switch (this.spout) {
                        case RICH_SPOUT:
                            topologyBuilder.setSpout(spout, utils.makeRichSpout());
                            break;
                        case SUPPLIER:
                            topologyBuilder.setSpout(spout, utils.makeSerializableSupplier());
                            break;
                        case UNSERIALIZABLE_RICH_SPOUT:
                            topologyBuilder.setSpout(spout, utils.makeUnserializableRichSpout());
                            break;
                        case UNSERIALIZABLE_SUPPLIER:
                            topologyBuilder.setSpout(spout, utils.makeUnserializableSupplier());
                            break;
                        case NULL_SPOUT:
                            topologyBuilder.setSpout(spout, (IRichSpout) null);
                    }

                    switch (this.bolt) {
                        case RICH_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeRichBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeRichBolt(),2).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeRichBolt(),3).shuffleGrouping(bolts[1]);
                            boltsCount=3;
                            break;
                        case UNSERIALIZABLE_RICH_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeUnserializableRichBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeUnserializableRichBolt()).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeUnserializableRichBolt()).shuffleGrouping(bolts[1]);
                            break;
                        case BASIC_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeBasicBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeBasicBolt(),2).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeBasicBolt(),3).shuffleGrouping(bolts[1]);
                            boltsCount=3;
                            break;
                        case UNSERIALIZABLE_BASIC_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeUnserializableBasicBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeUnserializableBasicBolt()).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeUnserializableBasicBolt()).shuffleGrouping(bolts[1]);
                            break;
                        case WINDOWED_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeWindowedBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeWindowedBolt(),2).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeWindowedBolt(),3).shuffleGrouping(bolts[1]);
                            boltsCount=3;
                            break;
                        case UNSERIALIZABLE_WINDOWED_BOLT:
                            topologyBuilder.setBolt(bolts[0], utils.makeUnserializableWindowedBolt()).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], utils.makeUnserializableWindowedBolt()).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], utils.makeUnserializableWindowedBolt()).shuffleGrouping(bolts[1]);
                            break;
                        case NULL_BOLT:
                            topologyBuilder.setBolt(bolts[0], (IRichBolt) null).shuffleGrouping(spout);
                            topologyBuilder.setBolt(bolts[1], (IRichBolt) null).shuffleGrouping(bolts[0]);
                            topologyBuilder.setBolt(bolts[2], (IRichBolt) null).shuffleGrouping(bolts[1]);
                    }

                    switch (statefulBolt) {
                        case STATEFUL_BOLT:
                            topologyBuilder.setBolt("statefulBolt", utils.makeStatefulBolt()).shuffleGrouping(spout);
                            expBolts.add("statefulBolt");
                            expSpout.add("$checkpointspout");
                            boltsCount++;
                            break;
                        case UNSERIALIZABLE_STATEFUL_BOLT:
                            topologyBuilder.setBolt("statefulBolt", utils.makeUnserializableStatefulBolt()).shuffleGrouping(spout);
                            break;
                        case STATEFUL_WINDOWED_BOLT:
                            topologyBuilder.setBolt("statefulBolt", utils.makeStatefulWindowedBolt()).shuffleGrouping(spout);
                            expBolts.add("statefulBolt");
                            expSpout.add("$checkpointspout");
                            boltsCount++;
                            break;
                        case UNSERIALIZABLE_STATEFUL_WINDOWED_BOLT:
                            topologyBuilder.setBolt("statefulBolt", utils.makeUnserializableStatefulWindowedBolt()).shuffleGrouping(spout);
                            break;
                        case NO_STATEFUL_BOLT:
                            break;
                        case NULL_BOLT:
                            topologyBuilder.setBolt("statefulBolt", (IStatefulBolt<? extends State>) null).shuffleGrouping(spout);
                            break;
                    }

                    switch (this.workerHook){
                        case WORKER_HOOK:
                            topologyBuilder.addWorkerHook(utils.makeWorkerHook());
                            break;
                        case UNSERIALIZABLE_WORKER_HOOK:
                            topologyBuilder.addWorkerHook(utils.makeUnserializableWorkerHook());
                            break;
                        case NULL_WORKER_HOOK:
                            topologyBuilder.addWorkerHook(null);
                            break;
                    }

                    StormTopology stormTopology = topologyBuilder.createTopology();
                    assertEquals(boltsCount, stormTopology.get_bolts_size());
                    assertEquals(ImmutableSet.copyOf(expBolts), stormTopology.get_bolts().keySet());
                    assertEquals(ImmutableSet.copyOf(expSpout), stormTopology.get_spouts().keySet());

                    if(this.statefulBolt==STATEFUL_BOLT || this.statefulBolt==STATEFUL_WINDOWED_BOLT) {
                        assertEquals(ImmutableSet.of(new GlobalStreamId(spout, "default"),
                                        new GlobalStreamId("$checkpointspout", "$checkpoint")),
                                stormTopology.get_bolts().get(bolts[0]).get_common().get_inputs().keySet());
                        assertEquals(ImmutableSet.of(new GlobalStreamId(bolts[0], "default"),
                                        new GlobalStreamId(bolts[0], "$checkpoint")),
                                stormTopology.get_bolts().get(bolts[1]).get_common().get_inputs().keySet());
                        assertEquals(ImmutableSet.of(new GlobalStreamId(bolts[1], "default"),
                                        new GlobalStreamId(bolts[1], "$checkpoint")),
                                stormTopology.get_bolts().get(bolts[2]).get_common().get_inputs().keySet());
                        assertEquals(ImmutableSet.of(new GlobalStreamId(spout, "default"),
                                        new GlobalStreamId("$checkpointspout", "$checkpoint")),
                                stormTopology.get_bolts().get("statefulBolt").get_common().get_inputs().keySet());
                    }else{
                        assertEquals(ImmutableSet.of(new GlobalStreamId(spout, "default")),
                                stormTopology.get_bolts().get(bolts[0]).get_common().get_inputs().keySet());
                        assertEquals(ImmutableSet.of(new GlobalStreamId(bolts[0], "default")),
                                stormTopology.get_bolts().get(bolts[1]).get_common().get_inputs().keySet());
                        assertEquals(ImmutableSet.of(new GlobalStreamId(bolts[1], "default")),
                                stormTopology.get_bolts().get(bolts[2]).get_common().get_inputs().keySet());
                    }

                    assertEquals(0, stormTopology.get_bolts().get(bolts[0]).get_common().get_parallelism_hint());       //PIT
                    assertEquals(2, stormTopology.get_bolts().get(bolts[1]).get_common().get_parallelism_hint());       //PIT
                    assertEquals(3, stormTopology.get_bolts().get(bolts[2]).get_common().get_parallelism_hint());       //PIT
                    assertTrue(stormTopology.is_set_worker_hooks());
                    assertEquals(1,stormTopology.get_worker_hooks_size());

                }
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class SetBoltTest {
        private TopologyBuilder topologyBuilder;
        private final Boolean serializable;
        private final int parallelismHint;
        private final Utils util = new Utils();
        private final boolean nullException;
        private final boolean expException;
        private final String id;

        public SetBoltTest(String id, Boolean serializable, int parallelismHint, boolean nullException, boolean expException) {
            this.id = id;
            this.serializable = serializable;
            this.parallelismHint = parallelismHint;
            this.nullException = nullException;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {"component", true, 1, false, true},
                    {"component1", true, 1, false, false},
                    {"", false, -1, false, true},
                    {null, null, 0, true, true},
            });
        }

        @Before
        public void startup() {
            topologyBuilder = new TopologyBuilder();
            topologyBuilder.setBolt("component", util.makeRichBolt());
        }

        @Test
        public void setBasicBoldTest() {
            try {
                BoltDeclarer boltDeclarer;
                if (this.serializable == null) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, (IBasicBolt) null, this.parallelismHint);
                } else if (this.serializable) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeBasicBolt(), this.parallelismHint);
                } else {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeUnserializableBasicBolt(), this.parallelismHint);
                }
                assertNotNull(boltDeclarer);
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void setRichBoldTest() {
            try {
                BoltDeclarer boltDeclarer;
                if (this.serializable == null) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, (IRichBolt) null, this.parallelismHint);
                } else if (this.serializable) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeRichBolt(), this.parallelismHint);
                } else {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeUnserializableRichBolt(), this.parallelismHint);
                }
                assertNotNull(boltDeclarer);
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void setStatefulBoldTest() {
            try {
                BoltDeclarer boltDeclarer;
                if (this.serializable == null) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, (IStatefulBolt<? extends State>) null, this.parallelismHint);
                } else if (this.serializable) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeStatefulBolt(), this.parallelismHint);
                } else {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeUnserializableStatefulBolt(), this.parallelismHint);
                }
                assertNotNull(boltDeclarer);
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void setStatefulWindowedBoldTest() {
            try {
                BoltDeclarer boltDeclarer;
                if (this.serializable == null) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, (IStatefulWindowedBolt<? extends State>) null, this.parallelismHint);
                } else if (this.serializable) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeStatefulPersistentWindowedBolt(), this.parallelismHint); //aumenta coverage jacoco
                } else {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeUnserializableStatefulWindowedBolt(), this.parallelismHint);
                }
                assertNotNull(boltDeclarer);
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void setWindowedBoldTest() {
            try {
                BoltDeclarer boltDeclarer;
                if (this.serializable == null) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, (IWindowedBolt) null, this.parallelismHint);
                } else if (this.serializable) {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeWindowedBolt(), this.parallelismHint);
                } else {
                    boltDeclarer = topologyBuilder.setBolt(this.id, util.makeUnserializableWindowedBolt(), this.parallelismHint);
                }
                assertNotNull(boltDeclarer);
            } catch (NullPointerException e) {
                assertTrue(this.nullException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class SetSpoutTest {
        private TopologyBuilder topologyBuilder;
        private final Boolean serializable;
        private final int parallelismHint;
        private final Utils util = new Utils();
        private final boolean nullException;
        private final boolean expException;
        private final String id;

        public SetSpoutTest(String id, Boolean serializable, int parallelismHint, boolean nullException, boolean expException) {
            this.id = id;
            this.serializable = serializable;
            this.parallelismHint = parallelismHint;
            this.nullException = nullException;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {"component", true, 1, false, true},
                    {"component1", true, 1, false, false},
                    {"", false, -1, false, true},
                    {null, null, 0, true, true},
            });
        }

        @Before
        public void startup() {
            topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("component", util.makeRichSpout());
        }

        @Test
        public void setRichSpoutTest(){
            try {
                SpoutDeclarer spoutDeclarer;
                if (this.serializable == null) {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, (IRichSpout) null, this.parallelismHint);
                } else if (this.serializable) {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, util.makeRichSpout(), this.parallelismHint);
                } else {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, util.makeUnserializableRichSpout(), this.parallelismHint);
                }
                assertNotNull(spoutDeclarer);
            }catch(NullPointerException e){
                assertTrue(this.nullException);
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }

        @Test
        public void setSerializableSupplierTest(){
            try {
                SpoutDeclarer spoutDeclarer;
                if (this.serializable == null) {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, (SerializableSupplier<?>) null, this.parallelismHint);
                } else if (this.serializable) {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, util.makeSerializableSupplier(), this.parallelismHint);
                } else {
                    spoutDeclarer=topologyBuilder.setSpout(this.id, util.makeUnserializableSupplier(), this.parallelismHint);
                }
                assertNotNull(spoutDeclarer);
            }catch(NullPointerException e){
                assertTrue(this.nullException);
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class CreateTopology2Test {

        public CreateTopology2Test(){

        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){
            return Arrays.asList(new Object[][]{
                    {},
            });
        }



        @Test
        public void topologyBuilderTest() {

            String[] bolts = new String[]{"bolt1", "bolt2", "bolt3"};
            String spout = "spout";
            Utils utils=new Utils();
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            topologyBuilder.setSpout(spout, utils.makeRichSpout()).addSharedMemory(new SharedMemory("pippo"));

            topologyBuilder.setBolt(bolts[0], utils.makeBasicBolt()).shuffleGrouping(spout);
            topologyBuilder.setBolt(bolts[1], utils.makeBasicBolt(), 2).shuffleGrouping(bolts[0]);
            topologyBuilder.setBolt(bolts[2], utils.makeBasicBolt(), 3).shuffleGrouping(bolts[1]);

            topologyBuilder.setBolt("statefulBolt", utils.makeStatefulBolt()).shuffleGrouping(spout);

            topologyBuilder.addWorkerHook(utils.makeWorkerHook());


            StormTopology stormTopology = topologyBuilder.createTopology();

            assertFalse(stormTopology.get_component_to_shared_memory().isEmpty());

            assertTrue(stormTopology.get_component_to_shared_memory().get("spout").contains("pippo"));
            assertTrue(stormTopology.is_set_shared_memory());
        }
    }
}
