import number1.HostMeanProcess;
import number2.NCProcess;
import number5.Number5Process;

public class Main {

    public static void main(String[] args) throws Exception {
        // Host Mean
        HostMeanProcess hostMeanProcess = new HostMeanProcess();
        hostMeanProcess.run("input/airbnb", "output/number1");

        // Neighbourhood Count
        NCProcess ncProcess = new NCProcess();
        ncProcess.run("input/airbnb", "output/number2");

        // mean price & max price & min price groupby roomtype.

        // mean Minimum night groupby room_type & host_id

        // Max House Count per host_id on Each Neighbourhood.
        Number5Process number5Process = new Number5Process();
        number5Process.run("input/airbnb", "output/number5");
    }

}