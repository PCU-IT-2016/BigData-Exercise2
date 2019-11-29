import java.util.StringTokenizer;

public class Test {
    public static void main(String args[]) {
        String s = "1302185,\"1-5pax Paya Lebar MRT, Sentosa, MBS, Orchard, City\",7080517,Ruth Mandy,Central Region,Geylang,1.31511,103.88947,Entire home/apt,129,30,83,2019-05-11,1.11,11,348";
        String []splitted = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for (String v : splitted) {
            System.out.println(v);
        }
        System.out.println("------------------------------------------");
//        StringTokenizer itr = new StringTokenizer(s);
//        while (itr.hasMoreTokens())
//            System.out.println(itr.nextToken());
    }
}
