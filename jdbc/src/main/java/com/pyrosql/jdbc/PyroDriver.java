package com.pyrosql.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC Driver implementation for PyroSQL.
 *
 * Connection URL format: jdbc:pyrosql://host:port/dbname
 *
 * The driver is automatically registered via the java.sql.Driver SPI service.
 */
public class PyroDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:pyrosql://";
    private static final Pattern URL_PATTERN = Pattern.compile(
            "^jdbc:pyrosql://([^:/]+)(?::(\\d+))?(?:/([^?]*))?(?:\\?(.*))?$"
    );
    private static final int DEFAULT_PORT = 12520;

    static {
        try {
            DriverManager.registerDriver(new PyroDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register PyroSQL JDBC driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        Matcher m = URL_PATTERN.matcher(url);
        if (!m.matches()) {
            throw new SQLException("Invalid PyroSQL JDBC URL: " + url);
        }

        String host = m.group(1);
        int port = m.group(2) != null ? Integer.parseInt(m.group(2)) : DEFAULT_PORT;
        String database = m.group(3) != null ? m.group(3) : "";

        // Parse query string parameters
        String query = m.group(4);
        Properties merged = new Properties();
        if (info != null) merged.putAll(info);
        if (query != null && !query.isEmpty()) {
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2) merged.setProperty(kv[0], kv[1]);
            }
        }

        String user = merged.getProperty("user", "");
        String password = merged.getProperty("password", "");

        return new PyroConnection(host, port, database, user, password, merged);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo userProp = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
        userProp.description = "Database user name";
        userProp.required = true;

        DriverPropertyInfo passProp = new DriverPropertyInfo("password", null);
        passProp.description = "Database user password";
        passProp.required = true;

        DriverPropertyInfo timeoutProp = new DriverPropertyInfo("loginTimeout", "30");
        timeoutProp.description = "Login timeout in seconds";
        timeoutProp.required = false;

        return new DriverPropertyInfo[]{userProp, passProp, timeoutProp};
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging not used");
    }
}
