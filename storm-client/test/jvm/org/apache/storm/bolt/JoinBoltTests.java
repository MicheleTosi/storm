package org.apache.storm.bolt;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class JoinBoltTests {
    static String[] studentFields = { "studentId", "name", "surname", "university" };
    static Object[][] students = {
            { 1, "mario", "rossi", "sapienza" },
            { 2, "franco", "verdi", "tor vergata" },
            { 3, "andrea", "bianchi", "d'annunzio" },
            { 4, "filippo", "ricci", "oxford" },
            { 5, "giorgia", "neri", "tor vergata" }
    };

    static String[] universityFields = { "universityId", "university", "city" };

    static Object[][] universities = {
            { 1, "sapienza", "roma" },
            { 2, "tor vergata", "roma" },
            { 3, "d'annunzio", "pescara" },
            { 4, "oxford", "oxford" }
    };

    static String[] cityFields = { "cityId", "city", "country"};
    static Object[][] cities = {
            { 1, "roma", "Italy" },
            { 2, "pescara", "Italy" },
            { 3, "oxford", "UK" }
    };

    private static void printResults(MockCollector collector) {
        int counter = 0;
        for (List<Object> rec : collector.actualResults) {
            System.out.print(++counter + ") ");
            for (Object field : rec) {
                System.out.print(field + ", ");
            }
            System.out.println("");
        }
    }

    private static TupleWindow makeTupleWindow(ArrayList<Tuple> stream) {
        return new TupleWindowImpl(stream, null, null);
    }

    @SafeVarargs
    private static TupleWindow makeTupleWindow(ArrayList<Tuple>... streams) {
        ArrayList<Tuple> combined = null;
        for (int i = 0; i < streams.length; i++) {
            if (i == 0) {
                combined = new ArrayList<>(streams[0]);
            } else {
                combined.addAll(streams[i]);
            }
        }
        return new TupleWindowImpl(combined, null, null);
    }

    private static ArrayList<Tuple> makeStream(String streamName, String[] fieldNames, Object[][] data, String srcComponentName) {
        ArrayList<Tuple> result = new ArrayList<>();
        MockContext mockContext = new MockContext(fieldNames);

        for (Object[] record : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(record), srcComponentName, 0, streamName);
            result.add(rec);
        }

        return result;
    }

    private static ArrayList<Tuple> makeNestedEventsStream(String streamName, String[] fieldNames, Object[][] records
            , String srcComponentName) {

        MockContext mockContext = new MockContext(new String[]{ "outer" });
        ArrayList<Tuple> result = new ArrayList<>(records.length);

        // convert each record into a HashMap using fieldNames as keys
        for (Object[] record : records) {
            HashMap<String, Object> recordMap = new HashMap<>(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                recordMap.put(fieldNames[i], record[i]);
            }

            ArrayList<Object> tupleValues = new ArrayList<>(1);
            tupleValues.add(recordMap);
            TupleImpl tuple = new TupleImpl(mockContext, tupleValues, srcComponentName, 0, streamName);
            result.add(tuple);
        }

        return result;
    }

    @RunWith(Parameterized.class)
    public static class  SelectTest {
        private final boolean expException;
        private final String commaSeparatedKeys;

        public SelectTest(String commaSeparatedKeys, boolean expException) {
            this.commaSeparatedKeys = commaSeparatedKeys;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {null, true},
                    {"", false},
                    {"studentId,name,surname,university", false},
                    {"city", false},
            });
        }

        @Test
        public void testSelect() {
            try {
                ArrayList<Tuple> studentStream = makeStream("students", studentFields, students, "studentsSpout");
                TupleWindow window = makeTupleWindow(studentStream);

                JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "students", studentFields[0])
                        .select(this.commaSeparatedKeys);
                MockCollector collector = new MockCollector();
                bolt.prepare(null, null, collector);
                bolt.execute(window);
                printResults(collector);
                assertEquals(studentStream.size(), collector.actualResults.size());
                assertFalse(this.expException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class  NestedKeysSelectTest{
        private final boolean expException;
        private final String commaSeparatedKeys;

        public NestedKeysSelectTest(String commaSeparatedKeys, boolean expException){
            this.commaSeparatedKeys=commaSeparatedKeys;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {null, true},
                    {"", false},
                    {"outer.university,outer.city", false},
                    {"university,city", false},
            });
        }

        @Test
        public void testNestedKeys() {
            try {
                ArrayList<Tuple> universityStream = makeNestedEventsStream("university", universityFields, universities, "universitySpout");
                TupleWindow window = makeTupleWindow(universityStream);
                JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "university", "outer.universityId")
                        .select(this.commaSeparatedKeys);
                MockCollector collector = new MockCollector();
                bolt.prepare(null, null, collector);
                bolt.execute(window);
                printResults(collector);
                assertEquals(universityStream.size(), collector.actualResults.size());
                assertFalse(this.expException);
            }catch(NullPointerException e){
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class  ProjectionTest {

        private final String newStream;
        private final String priorStream;
        private final String field;
        private final boolean expException;
        private final int expSizeJoin;
        private final int expSizeLeftJoin;

        public ProjectionTest(String newStream, String field, String priorStream, int expSizeJoin,int expSizeLeftJoin, boolean expException){
            this.newStream=newStream;
            this.field=field;
            this.priorStream=priorStream;
            this.expException=expException;
            this.expSizeJoin=expSizeJoin;
            this.expSizeLeftJoin=expSizeLeftJoin;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    //aggiunto il campo che conta il numero di tuple che si ottengono dal join per aumentare la coverage
                    {null,null,null,0,0,true},
                    {null,cityFields[0],null,0,0,true},
                    {null, universityFields[0],null,0,0,true},
                    {null,null,"",0,0,true},
                    {null,cityFields[0],"",0,0,true},
                    {null, universityFields[0],"",0,0,true},
                    {null,null,"university",0,0,true},
                    {null,cityFields[0],"university",0,0,true},
                    {null, universityFields[0],"university",0,0,true},
                    {null,null,"student",0,0,true},
                    {null,cityFields[0],"student",0,0,true},
                    {null, universityFields[0],"student",0,0,true},
                    {null,null,"city",0,0,true},
                    {null,cityFields[0],"city",0,0,true},
                    {null, universityFields[0],"city",0,0,true},
                    {"",null,null,0,0,true},
                    {"",cityFields[0],null,0,0,true},
                    {"", universityFields[0],null,0,0,true},
                    {"",null,"",0,0,true},
                    {"",cityFields[0],"",0,0,true},
                    {"", universityFields[0],"",0,0,true},
                    {"",null,"university",0,0,true},
                    {"",cityFields[0],"university",0,0,true},
                    {"", universityFields[0],"university",0,0,true},
                    {"",null,"student",0,0,true},
                    {"",cityFields[0],"student",0,0,true},
                    {"", universityFields[0],"student",0,0,true},
                    {"",null,"city",0,0,true},
                    {"",cityFields[0],"city",0,0,true},
                    {"", universityFields[0],"city",0,0,true},
                    {"university",null,null,0,0,true},
                    {"university",cityFields[0],null,0,0,true},
                    {"university", universityFields[0],null,0,0,true},
                    {"university",studentFields[0],null,0,0,true},
                    {"university",null,"",0,0,true},
                    {"university",cityFields[0],"",0,0,true},
                    {"university", universityFields[0],"",0,0,true},
                    {"university",studentFields[0],"",0,0,true},
                    {"university",null,"university",0,0,true},
                    {"university",cityFields[0],"university",0,0,true},
                    {"university", universityFields[0],"university",0,0,true},
                    {"university",studentFields[0],"university",0,0,true},
                    {"university",null,"student",0,0,true},
                    {"university",universityFields[0],"student",0,students.length,false},
                    {"university",studentFields[0],"student",0,students.length,false},
                    {"university",studentFields[3],"student",students.length,students.length,false},
                    {"university",cityFields[0],"student",0,students.length,false},
                    {"university",null,"city",0,0,true},
                    {"university",cityFields[0],"city",0,0,true},
                    {"university", universityFields[0],"city",0,0,true},
                    {"university",studentFields[0],"city",0,0,true},
                    {"student",null,null,0,0,true},
                    {"student",cityFields[0],null,0,0,true},
                    {"student",studentFields[0],null,0,0,true},
                    {"student", universityFields[0],null,0,0,true},
                    {"student",null,"",0,0,true},
                    {"student",cityFields[0],"",0,0,true},
                    {"student",studentFields[0],"",0,0,true},
                    {"student", universityFields[0],"",0,0,true},
                    {"student",null,"university",0,0,true},
                    {"student",studentFields[0],"university",0,0,true},
                    {"student",universityFields[0],"university",0,0,true},
                    {"student",studentFields[3],"university",0,0,true},
                    {"student",cityFields[0],"university",0,0,true},
                    {"student",null,"student",0,0,true},
                    {"student",cityFields[0],"student",0,0,true},
                    {"student",studentFields[0],"student",0,0,true},
                    {"student", universityFields[0],"student",0,0,true},
                    {"student",null,"city",0,0,true},
                    {"student",cityFields[0],"city",0,0,true},
                    {"student",studentFields[0],"city",0,0,true},
                    {"student", universityFields[0],"city",0,0,true},
                    {"city",null,null,0,0,true},
                    {"city",cityFields[0],null,0,0,true},
                    {"city", universityFields[0],null,0,0,true},
                    {"city",null,"",0,0,true},
                    {"city",cityFields[0],"",0,0,true},
                    {"city", universityFields[0],"",0,0,true},
                    {"city",null,"university",0,0,true},
                    {"city",cityFields[0],"university",0,0,true},
                    {"city", universityFields[0],"university",0,0,true},
                    {"city",null,"student",0,0,true},
                    {"city",cityFields[0],"student",0,0,true},
                    {"city", universityFields[0],"student",0,0,true},
                    {"city",null,"city",0,0,true},
                    {"city",cityFields[0],"city",0,0,true},
                    {"city", universityFields[0],"city",0,0,true},
            });
        }

        @Test
        public void joinTest(){
            try {
                ArrayList<Tuple> studentStream=makeStream("student", studentFields, students, "studentSpout");
                ArrayList<Tuple> universityStream = makeStream("university", universityFields, universities, "universitySpout");
                TupleWindow window=makeTupleWindow(studentStream, universityStream);

                JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "student", studentFields[3])
                        .join(this.newStream, this.field, this.priorStream)
                        .select("name,surname,university");

                MockCollector collector = new MockCollector();
                bolt.prepare(null, null, collector);
                bolt.execute(window);
                printResults(collector);

//                assertEquals(this.expSizeJoin, collector.actualResults.size()); //PIT
                for (List<Object> tuple : collector.actualResults) {
                    assertEquals(3, tuple.size());
                    for (Object o : tuple) {
                        assertNotNull(o);
                    }
                }

                assertFalse(this.expException);
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }

        @Test
        public void leftJoinTest(){
            try {
                ArrayList<Tuple> studentStream=makeStream("student", studentFields, students, "studentSpout");
                ArrayList<Tuple> universityStream = makeStream("university", universityFields, universities, "universitySpout");
                TupleWindow window=makeTupleWindow(studentStream, universityStream);

                JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "student", studentFields[3])
                        .leftJoin(this.newStream, this.field, this.priorStream)
                        .select("name,surname,university");

                MockCollector collector = new MockCollector();
                bolt.prepare(null, null, collector);
                bolt.execute(window);
                printResults(collector);

//                assertEquals(this.expSizeLeftJoin, collector.actualResults.size()); //PIT
                for (List<Object> tuple : collector.actualResults) {
                    assertEquals(3, tuple.size());
                    for (Object o : tuple) {
                        assertNotNull(o);
                    }
                }

                assertFalse(this.expException);
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }

    }

    static class MockCollector extends OutputCollector {
        public ArrayList<List<Object>> actualResults = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
            actualResults.add(tuple);
            return null;
        }

    } // class MockCollector

    static class MockContext extends GeneralTopologyContext {

        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, new HashMap<>(), null, null, null, null);
            this.fields = new Fields(fieldNames);
        }

        @Override
        public String getComponentId(int taskId) {
            return "component";
        }

        @Override
        public Fields getComponentOutputFields(String componentId, String streamId) {
            return fields;
        }

    }
}