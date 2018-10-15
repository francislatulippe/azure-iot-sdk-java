/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package tests.integration.com.microsoft.azure.sdk.iot.iothubservices;

import com.microsoft.azure.sdk.iot.common.TestConstants;
import com.microsoft.azure.sdk.iot.common.X509Cert;
import com.microsoft.azure.sdk.iot.common.helpers.Tools;
import com.microsoft.azure.sdk.iot.common.iothubservices.DeviceTwinCommon;
import com.microsoft.azure.sdk.iot.common.iothubservices.SubscribeToDesiredPropertiesErrorInjectionCommon;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.service.auth.AuthenticationType;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SubscribeToDesiredPropertiesErrorInjectionIT extends SubscribeToDesiredPropertiesErrorInjectionCommon
{
    public SubscribeToDesiredPropertiesErrorInjectionIT(String deviceId, String moduleId, IotHubClientProtocol protocol, AuthenticationType authenticationType, String clientType)
    {
        super(deviceId, moduleId, protocol, authenticationType, clientType);
    }

    //This function is run before even the @BeforeClass annotation, so it is used as the @BeforeClass method
    @Parameterized.Parameters(name = "{2} with {3} auth using {4}")
    public static Collection inputsCommons() throws IOException, GeneralSecurityException
    {
        return DeviceTwinIT.inputsCommons();
    }
}
