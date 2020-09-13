package com.eights.common.utils;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;

public class DateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateUtils.class);

    private static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static long stringToTimestamp(String dateStr, String pattern) {
        FastDateFormat dateFormat = FastDateFormat.getInstance(pattern);
        try {
            return dateFormat.parse(dateStr).getTime();
        } catch (ParseException e) {
            LOG.error(String.format("string parse timestamp error: [%s]", e.getMessage()));
        }

        return 0L;
    }

    public static String format(long milliseconds) {
        return DateFormatUtils.format(milliseconds, DEFAULT_PATTERN);
    }

    public static String format(Date date) {
        return format(date, DEFAULT_PATTERN);
    }

    public static String format(Date date, String pattern) {
        return DateFormatUtils.format(date, pattern);
    }
}
