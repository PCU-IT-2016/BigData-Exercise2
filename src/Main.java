import Processes.Number1Process;
import Processes.Number2Process;
import Processes.Number3Process;
import Processes.Number5Process;

public class Main {

    public static void main(String[] args) throws Exception {
        // Host Mean
        Number1Process hostMeanProcess = new Number1Process();
        hostMeanProcess.run("input/airbnb", "output/number1");

        // Neighbourhood Count
        Number2Process ncProcess = new Number2Process();
        ncProcess.run("input/airbnb", "output/number2");

        // mean price & max price & min price groupby roomtype.
        Number3Process number3Process = new Number3Process();
        number3Process.run("input/airbnb", "output/number3");

        // mean Minimum night groupby room_type & host_id

        // Max House Count per host_id on Each Neighbourhood.
        Number5Process number5Process = new Number5Process();
        number5Process.run("input/airbnb", "output/number5");
    }

}