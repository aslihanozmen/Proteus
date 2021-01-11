package crowd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.qcri.crowdservice.crowdclient.CrowdClient;
import org.qcri.crowdservice.crowdclient.CrowdClientException;
import org.qcri.crowdservice.crowdclient.PostMode;

import qa.qcri.crowdservice.crowdcommon.dao.GenericCrowdException;
import qa.qcri.crowdservice.crowdcommon.dao.Question;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class QuestionGenerator {

    public static void main(String[] args) {
        List<String> headersFrom = new ArrayList<String>();
        List<String> headersTo = new ArrayList<String>();
        Map<List<String>, HashSet<List<String>>> allExamples = new HashMap<List<String>, HashSet<List<String>>>();
        Collection<List<String>> queryKeys = new ArrayList<List<String>>();

        headersFrom.add("Airport code");
        headersFrom.add("System");
        headersTo.add("City");
        headersTo.add("Country");


        List<String> row1 = new ArrayList<String>();
        row1.add("FCO");
        row1.add("YATA");
        List<String> row1Answer = new ArrayList<String>();
        row1Answer.add("Rome");
        row1Answer.add("Italy");
        allExamples.put(row1, new HashSet<List<String>>());
        allExamples.get(row1).add(row1Answer);


        List<String> row2 = new ArrayList<String>();
        row2.add("CDG");
        row2.add("YATA");
        List<String> row2Answer = new ArrayList<String>();
        row2Answer.add("Paris");
        row2Answer.add("France");
        List<String> row2Answer2 = new ArrayList<String>();
        row2Answer2.add("Paris2");
        row2Answer2.add("France");
        allExamples.put(row2, new HashSet<List<String>>());
        allExamples.get(row2).add(row2Answer);
        allExamples.get(row2).add(row2Answer2);


        List<String> row4 = new ArrayList<String>();
        row4.add("CAI");
        row4.add("YATA");
        queryKeys.add(row4);

        List<String> row3 = new ArrayList<String>();
        row3.add("YYZ");
        row3.add("YATA");
        queryKeys.add(row3);


        QuestionGenerator qg = new QuestionGenerator();

        try {

            //FIXME Run Crowdclient and update URL and Port accordingly
            CrowdClient client = new CrowdClient("http://localhost:8880/DataXFormer_Crowd/");
            client.authenticate("admin", "admin");

            Question question = qg.generateQuestion(headersFrom, headersTo, allExamples, queryKeys);


            client.clearQuestionsByCategory("testXFormer");
            ArrayList<Question> questions = new ArrayList<>();
            questions.add(question);
            client.postQuestions(questions, "testXFormer", PostMode.INTERNAL);
            client.close();

        } catch (GenericCrowdException e) {
            e.printStackTrace();
        } catch (CrowdClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public ArrayList<Question> generateQuestionsRandomPacking(
            List<String> headersFrom, List<String> headersTo,
            Map<List<String>, HashSet<List<String>>> allExamples,
            Collection<List<String>> allQueryKeys,
            int examplesPerQuestion, int queryKeysPerQuestion) {
        int nq = (int) Math.ceil(allQueryKeys.size() * 1.0 / queryKeysPerQuestion);
        ArrayList<Question> questions = new ArrayList<Question>(nq);

        //First, divide the query keys (Randomly)
        ArrayList<List<String>> queryKeysShuffled = new ArrayList<>(allQueryKeys);
        Collections.shuffle(queryKeysShuffled);

        for (int i = 0; i < nq; i++) {
            Map<List<String>, HashSet<List<String>>> examples = new HashMap<>();

            ArrayList<List<String>> queryKeys = new ArrayList<List<String>>();
            for (int j = 0; j < queryKeysPerQuestion
                    && i * queryKeysPerQuestion + j < allQueryKeys.size(); j++) {
                queryKeys.add(queryKeysShuffled.get(i * queryKeysPerQuestion + j));
            }

            //Select random examples
            ArrayList<List<String>> exampleKeysShuffled = new ArrayList<>();
            exampleKeysShuffled.addAll(allExamples.keySet());

            for (int j = 0; j < examplesPerQuestion && j < exampleKeysShuffled.size(); j++) {
                List<String> exK = exampleKeysShuffled.get(j);
                examples.put(exK, allExamples.get(exK));
            }

            Question question = generateQuestion(headersFrom, headersTo, examples, queryKeys);
            questions.add(question);
        }// ENDFOR nq
        return questions;
    }


    public Question generateQuestion(
            List<String> headersFrom, List<String> headersTo,
            Map<List<String>, HashSet<List<String>>> examples,
            Collection<List<String>> queryKeys) {
        Question question = new Question();

        JsonObject content = new JsonObject();

        //Headers From
        JsonArray headersFromJsonArr = new JsonArray();
        for (String headerFrom : headersFrom) {
            headersFromJsonArr.add(new JsonPrimitive(headerFrom));
        }
        content.add("headersFrom", headersFromJsonArr);

        //Headers To
        JsonArray headersToJsonArr = new JsonArray();
        for (String headerTo : headersTo) {
            headersToJsonArr.add(new JsonPrimitive(headerTo));
        }
        content.add("headersTo", headersToJsonArr);


        //Example rows
        JsonArray exampleRowsJsonArr = new JsonArray();
        //For now just pick random 5 examples
        for (List<String> exampleKey : examples.keySet()) {

            JsonArray cellsFrom = new JsonArray();

            for (String k : exampleKey) {
                cellsFrom.add(new JsonPrimitive(k));
            }

            for (List<String> exampleValue : examples.get(exampleKey)) {
                JsonArray cellsTo = new JsonArray();

                for (String v : exampleValue) {
                    cellsTo.add(new JsonPrimitive(v));
                }
                JsonObject row = new JsonObject();

                row.add("cellsFrom", cellsFrom);
                row.add("cellsTo", cellsTo);
                exampleRowsJsonArr.add(row);
            }

        }
        content.add("givenRows", exampleRowsJsonArr);


        //Query rows
        JsonArray queryRowsJsonArr = new JsonArray();
        for (List<String> queryKey : queryKeys) {
            JsonArray cellsFrom = new JsonArray();

            for (String k : queryKey) {
                cellsFrom.add(new JsonPrimitive(k));
            }
            JsonObject row = new JsonObject();
            row.add("cellsFrom", cellsFrom);
            queryRowsJsonArr.add(row);
        }
        content.add("queryRows", queryRowsJsonArr);

        question.setStatus(Question.READY_FOR_POST);
        question.setContent(content);
        question.setType("ANALOGY_OPEN");
        return question;
    }

}
