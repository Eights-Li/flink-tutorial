package com.eights.common.utils;

import com.eights.common.constant.PropertiesConstants;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxMySqlUtils {

    private static final Logger LOG = LoggerFactory.getLogger(VertxMySqlUtils.class);

    public static SQLClient buildVertxMySqlClient(ParameterTool tools) {

        //build mysql async client params
        JsonObject mySqlClientConf = new JsonObject();
        mySqlClientConf.put(PropertiesConstants.VERTX_URL, tools.get(PropertiesConstants.MYSQL_URL))
                .put(PropertiesConstants.VERTX_DRIVER_CLASS, PropertiesConstants.MYSQL_JDBC_DRIVER)
                .put(PropertiesConstants.VERTX_MAX_POOL_SIZE, tools.getInt(PropertiesConstants.VERTX_MAX_POOL_SIZE))
                .put(PropertiesConstants.VERTX_USER, tools.get(PropertiesConstants.MYSQL_USER))
                .put(PropertiesConstants.VERTX_PWD, tools.get(PropertiesConstants.MYSQL_PWD));

        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(tools.getInt(PropertiesConstants.VERTX_EVENT_LOOP_SIZE));
        vertxOptions.setWorkerPoolSize(tools.getInt(PropertiesConstants.VERTX_WORKER_POOL_SIZE));
        Vertx vertx = Vertx.vertx(vertxOptions);

        return JDBCClient.createShared(vertx, mySqlClientConf);
    }

}
