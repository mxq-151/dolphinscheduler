import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.text.ParseException;
import java.util.Date;

public class TrigerFireTimeTest {

    @Test
    public void testGetFireTime() throws ParseException {
        CronTriggerImpl cronTrigger=new CronTriggerImpl("kk","kk"," 0/5 * * * * ? ");
        Date tmp=new Date();
        for (int i = 0; i < 10; i++) {
            tmp=cronTrigger.getFireTimeAfter(tmp);
            System.out.println(tmp);
        }

        System.out.println(new Date().getTime());

    }
}
