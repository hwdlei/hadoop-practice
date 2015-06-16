package hia.io;
import org.apache.hadoop.io.Text;


public class TextTest {
	private static void strPrint() {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		System.out.println(s.length());
		System.out.println(s.indexOf("\u0041"));
		System.out.println(s.indexOf("\uD801\uDC00"));
	}

	private static void textPrint() {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		System.out.println(t.getLength());
		System.out.println(t.find("\u0041"));
		System.out.println(t.find("\u00DF"));
		System.out.println(t.find("\u6771"));
		System.out.println(t.find("\uD801\uDC00"));
	}

	public static void main(String[] args) {
		strPrint();
		textPrint();
	}

}
