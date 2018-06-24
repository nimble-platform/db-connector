/**
 * Created by evgeniyh on 6/24/18.
 */

public class ManagerConfig {
    private final String driver;
    private final String user;
    private final String password;
    private final String url;

    public ManagerConfig(String driver, String user, String password, String url) {
        this.driver = driver;
        this.user = user;
        this.password = password;
        this.url = url;
    }

    public String getDriver() {
        return driver;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    public boolean isMissingAnyValue() {
        return isNullOrEmpty(driver) ||
                isNullOrEmpty(user) ||
                isNullOrEmpty(password) ||
                isNullOrEmpty(url);
    }
}
