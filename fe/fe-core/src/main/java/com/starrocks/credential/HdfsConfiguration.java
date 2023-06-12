package com.starrocks.credential;

import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudProperty;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HdfsConfiguration implements CloudConfiguration {
    Map<String, String> userOptions = new HashMap<>();
    public HdfsConfiguration(Map<String, String> userOptions) {
        this.userOptions = userOptions;
    }

    public Map<String, String> getUserOptions() {
        return userOptions;
    }

    public HdfsConfiguration setUserOptions(Map<String, String> userOptions) {
        this.userOptions = userOptions;
        return this;
    }

    public void applyToConfiguration(Configuration configuration) {
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        List<TCloudProperty> properties = new LinkedList<>();
        tCloudConfiguration.setCloud_type(TCloudType.AWS);
        for (Map.Entry<String, String> entry : userOptions.entrySet()) {
            properties.add(new TCloudProperty(entry.getKey(), entry.getValue()));
        }
        tCloudConfiguration.setCloud_properties(properties);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HdfsConfiguration{");
        sb.append("userOptions=").append(userOptions);
        sb.append('}');
        return sb.toString();
    }
}

