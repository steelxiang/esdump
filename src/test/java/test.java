public class test {

    public static void main(String[] args) {
        String str="asdf\nsdf";
        System.out.println(str);
        if(str.contains("\n")){
            System.out.println("lsdjg");
            String replace = str.replace("\n", "\\n");
            System.out.println(replace);
        }
    }
}
