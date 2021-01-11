package webforms.wrappers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ParameterFields {


    private Map<String, Map<String, String>> selectionFields = new HashMap<String, Map<String, String>>();
    private Set<String> inputFields = new HashSet<String>();
    private Map<String, String> hiddenFields = new HashMap<String, String>();
    private Map<String, String> fieldToCode = new HashMap<String, String>();

    public ParameterFields(Map<String, Map<String, String>> selectionF, Set<String> inputF, Map<String, String> hiddenF, Map<String, String> fieldToCode) {
        this.setSelectionFields(selectionF);
        this.setInputFields(inputF);
        this.setHiddenFields(hiddenF);
        this.setFieldToCode(fieldToCode);
    }

    public Map<String, Map<String, String>> getSelectionFields() {
        return selectionFields;
    }

    public void setSelectionFields(Map<String, Map<String, String>> selectionFields) {
        this.selectionFields = selectionFields;
    }

    public Set<String> getInputFields() {
        return inputFields;
    }

    public void setInputFields(Set<String> inputFields) {
        this.inputFields = inputFields;
    }

    public Map<String, String> getHiddenFields() {
        return hiddenFields;
    }

    public void setHiddenFields(Map<String, String> hiddenFields) {
        this.hiddenFields = hiddenFields;
    }

    public Map<String, String> getFieldToCode() {
        return fieldToCode;
    }

    public void setFieldToCode(Map<String, String> fieldToCode) {
        this.fieldToCode = fieldToCode;
    }


}
