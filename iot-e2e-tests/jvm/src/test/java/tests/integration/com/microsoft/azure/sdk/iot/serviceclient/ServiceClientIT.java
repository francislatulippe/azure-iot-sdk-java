/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package tests.integration.com.microsoft.azure.sdk.iot.serviceclient;

import com.microsoft.azure.sdk.iot.common.TestConstants;
import com.microsoft.azure.sdk.iot.common.helpers.Tools;
import com.microsoft.azure.sdk.iot.common.serviceclient.ServiceClientCommon;
import com.microsoft.azure.sdk.iot.service.IotHubServiceClientProtocol;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.microsoft.azure.sdk.iot.common.helpers.Tools.retrieveEnvironmentVariableValue;


@RunWith(Parameterized.class)
public class ServiceClientIT extends ServiceClientCommon
{
    //This function is run before even the @BeforeClass annotation, so it is used as the @BeforeClass method
    @Parameterized.Parameters(name = "{0}")
    public static Collection inputsCommon()
    {
        iotHubConnectionString =
                Tools.retrieveEnvironmentVariableValue(TestConstants.IOT_HUB_CONNECTION_STRING_ENV_VAR_NAME);
        invalidCertificateServerConnectionString =
                Tools.retrieveEnvironmentVariableValue(TestConstants.UNTRUSTWORTHY_IOT_HUB_CONNECTION_STRING_ENV_VAR_NAME);
        return ServiceClientCommon.inputsCommon();
    }

    public ServiceClientIT(IotHubServiceClientProtocol protocol)
    {
        super(protocol);
    }


}
