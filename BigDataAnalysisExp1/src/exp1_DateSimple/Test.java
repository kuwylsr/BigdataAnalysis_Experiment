package exp1_DateSimple;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class Test {

	public static void main(String[] args) throws ParseException {
		StringBuilder s = new StringBuilder();
		s.append("aaaaa");
		s.append("bbbbbb");
		System.out.println(s);
		String a = "48870917|9.834235|57.119826|8.294238|2011/12/05|-1.7℃|?|12277|1975-06-28|Germany|farmer|?";
		System.out.println(a.replaceAll("\\?$", "aaa"));
		//System.out.println(a.replaceAll("\\|\\?\\|", "aaa"));
		
		String pattern1 = "[\\d]+\\-[\\d]+\\-[\\d]+";
		String pattern2 = "[\\d]+\\/[\\d]+\\/[\\d]+";
		String pattern3 = "[a-zA-Z]+\\s[\\d]+,[\\d]+";

		Pattern p1 = Pattern.compile(pattern1);
		Pattern p2 = Pattern.compile(pattern2);
		Pattern p3 = Pattern.compile(pattern3);
		
		Matcher m1 = p1.matcher("2012/02-22");
		Matcher m2 = p2.matcher("2012-02/14");
		Matcher m3 = p3.matcher("match 12,2012");
		
		System.out.println(m1.matches());
		System.out.println(m2.matches());
		System.out.println(m3.matches());
		
		
		
		LocalDate date = LocalDate.of(2001, 11, 2);
		System.out.println(date);

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd",Locale.US);
		Date datetest = dateFormat.parse("2012/02/12");
		//Date datetest1 = dateFormat.parse("2012-02-04");
		System.out.println(datetest.getTime());
		System.out.println(datetest.toString());
		System.out.println(datetest.toGMTString());
		System.out.println(datetest.toLocaleString());
		
	}
	

}

//public class Test {
//    public static void main(String[] args) throws ParseException {
//    	Test tdf = new Test();
//        tdf.dateFormat();
//    }
//    /**
//     * 对SimpleDateFormat类进行测试
//     * @throws ParseException 
//     */
//    public void dateFormat() throws ParseException{
//         //创建日期 
//        Date date = new Date(); 
//
//        //创建不同的日期格式 
//        DateFormat df1 = DateFormat.getInstance(); 
//        DateFormat df2 = new SimpleDateFormat("yyyy-MM-01 hh:mm:ss EE"); 
//        DateFormat df3 = DateFormat.getDateInstance(DateFormat.FULL, Locale.CHINA);     //产生一个指定国家指定长度的日期格式，长度不同，显示的日期完整性也不同 
//        DateFormat df4 = new SimpleDateFormat("yyyy年MM月dd日 hh时mm分ss秒 EE", Locale.CHINA); 
//        DateFormat df5 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss EEEEEE", Locale.US); 
//        DateFormat df6 = new SimpleDateFormat("yyyy-MM-dd");  
//
//        //将日期按照不同格式进行输出 
//        System.out.println("-------将日期按照不同格式进行输出------"); 
//        System.out.println("按照Java默认的日期格式，默认的区域                      : " + df1.format(date)); 
//        System.out.println("按照指定格式 yyyy-MM-dd hh:mm:ss EE ，系统默认区域      :" + df2.format(date)); 
//        System.out.println("按照日期的FULL模式，区域设置为中文                      : " + df3.format(date)); 
//        System.out.println("按照指定格式 yyyy年MM月dd日 hh时mm分ss秒 EE ，区域为中文 : " + df4.format(date)); 
//        System.out.println("按照指定格式 yyyy-MM-dd hh:mm:ss EE ，区域为美国        : " + df5.format(date)); 
//        System.out.println("按照指定格式 yyyy-MM-dd ，系统默认区域                  : " + df6.format(date)); 
//        
//        //将符合该格式的字符串转换为日期，若格式不相配，则会出错 
//        //Date date1 = df1.parse("16-01-24 下午2:32"); 
//        //Date date2 = df2.parse("2016-01-24 02:51:07 星期日"); 
//        //Date date3 = df3.parse("2016年01月24日 星期五"); 
//        Date date4 = df4.parse("2016年01月24日 02时51分18秒 星期日"); 
//        Date date5 = df5.parse("2016-01-24 02:51:18 Sunday"); 
//        Date date6 = df6.parse("2016-01-24"); 
//
//        System.out.println("-------输出将字符串转换为日期的结果------"); 
//        //System.out.println(date1); 
//        //System.out.println(date2); 
//        //System.out.println(date3); 
//        System.out.println(date4); 
//        System.out.println(date5); 
//        System.out.println(date6); 
//    }
//}
