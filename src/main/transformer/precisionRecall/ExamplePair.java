package main.transformer.precisionRecall;

import java.util.List;
import java.util.StringJoiner;

public class ExamplePair {

    private List<String> inputs;
    private String output;

    public ExamplePair(List<String> inputs, String output) {
        this.inputs = inputs;
        this.output = output;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ExamplePair.class.getSimpleName() + "[", "]")
            .add("input='" + inputs + "'")
            .add("output='" + output + "'")
            .toString();
    }
}
