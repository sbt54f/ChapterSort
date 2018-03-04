import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) {
		Pattern p = Pattern.compile("(chapter \\S+)\\.", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher("CHAPTER I. Down the Rabbit-Hole");
		if(m.find())
		{
			System.out.println(m.group(1));
		}
		System.out.println(m.find());

	}

}
