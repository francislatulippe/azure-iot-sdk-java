/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package tests.integration.com.microsoft.azure.sdk.iot.iothubservices;

import com.microsoft.azure.sdk.iot.common.iothubservices.GetDeviceTwinErrorInjectionCommon;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.service.auth.AuthenticationType;

public class GetDeviceTwinErrorInjectionIT extends GetDeviceTwinErrorInjectionCommon
{
    public GetDeviceTwinErrorInjectionIT(String deviceId, String moduleId, IotHubClientProtocol protocol, AuthenticationType authenticationType, String clientType)
    {
        super(deviceId, moduleId, protocol, authenticationType, clientType);
    }
}
