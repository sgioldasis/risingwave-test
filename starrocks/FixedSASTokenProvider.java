package com.risingwave.starrocks;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.security.AccessControlException;

/** Adapts Hadoop's fixed SAS token to StarRocks' no-argument provider loader. */
public final class FixedSASTokenProvider implements SASTokenProvider {
    private String token;

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        token = configuration.get("fs.azure.sas.fixed.token." + accountName);
        if (token == null || token.isEmpty()) {
            throw new IOException("Missing fixed SAS token for account " + accountName);
        }
        if (!token.startsWith("?")) {
            token = "?" + token;
        }
    }

    @Override
    public String getSASToken(
            String accountName,
            String fileSystem,
            String path,
            String operation) throws IOException, AccessControlException {
        if (token == null || token.isEmpty()) {
            throw new IOException("SAS token provider was not initialized");
        }
        return token;
    }
}
