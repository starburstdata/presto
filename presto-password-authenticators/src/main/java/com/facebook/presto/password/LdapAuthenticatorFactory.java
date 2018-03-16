/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.password;

import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.client.BasicAuthRequestFilter;

import java.util.Map;

import static com.facebook.presto.password.LdapConfig.INTERNAL_LDAP_PASSWORD_CONFIG;
import static com.facebook.presto.password.LdapConfig.INTERNAL_LDAP_USER_CONFIG;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class LdapAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "ldap";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(LdapConfig.class);
                        binder.bind(LdapAuthenticator.class).in(Scopes.SINGLETON);

                        if (config.get(INTERNAL_LDAP_USER_CONFIG) != null) {
                            httpClientBinder(binder).bindGlobalFilter(new BasicAuthRequestFilter(config.get(INTERNAL_LDAP_USER_CONFIG), config.get(INTERNAL_LDAP_PASSWORD_CONFIG)));
                        }
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(LdapAuthenticator.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
