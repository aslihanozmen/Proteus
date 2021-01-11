package lookup;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import services.SyntaxicMatch;
import services.SyntaxicMatch$;

import java.util.Arrays;

public class StringSolverTest {

    @Test
    void validationSolverSolutionOk() {
        SyntaxicMatch.TransformMultipleInputs interOuputTransformer =
                SyntaxicMatch$.MODULE$.transformMultipleInputs(Arrays.asList("0.82", "10 USD"), "0.82 * 10");

        String response=interOuputTransformer.how().apply(Arrays.asList("0.81", "20 USD"));
        Assertions.assertThat(response).isEqualTo("0.81 * 20");
    }
}