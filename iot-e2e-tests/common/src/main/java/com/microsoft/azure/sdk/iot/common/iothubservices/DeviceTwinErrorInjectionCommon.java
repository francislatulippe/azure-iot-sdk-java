/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.sdk.iot.common.iothubservices;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.sdk.iot.common.DeviceConnectionString;
import com.microsoft.azure.sdk.iot.common.ErrorInjectionHelper;
import com.microsoft.azure.sdk.iot.common.MessageAndResult;
import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Device;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Property;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.TwinPropertyCallBack;
import com.microsoft.azure.sdk.iot.device.exceptions.ModuleClientException;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.microsoft.azure.sdk.iot.service.auth.AuthenticationType;
import com.microsoft.azure.sdk.iot.service.devicetwin.*;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubException;
import org.junit.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.microsoft.azure.sdk.iot.device.IotHubClientProtocol.*;
import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK;
import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK_EMPTY;
import static com.microsoft.azure.sdk.iot.service.auth.AuthenticationType.SAS;
import static com.microsoft.azure.sdk.iot.service.auth.AuthenticationType.SELF_SIGNED;
import static org.junit.Assert.*;

public class DeviceTwinErrorInjectionCommon extends MethodNameLoggingIntegrationTest
{
    // Max time to wait to see it on Hub
    private static final long PERIODIC_WAIT_TIME_FOR_VERIFICATION = 100; // 0.1 sec
    private static final long MAX_WAIT_TIME_FOR_VERIFICATION = 60000; // 60 sec
    private static final long DELAY_BETWEEN_OPERATIONS = 200; // 0.2 sec

    private static final long MAXIMUM_TIME_FOR_IOTHUB_PROPAGATION_BETWEEN_DEVICE_SERVICE_CLIENTS = DELAY_BETWEEN_OPERATIONS * 10; // 2 sec

    //Max time to wait before timing out test
    private static final long MAX_MILLISECS_TIMEOUT_KILL_TEST = 120000; // 2 min

    // Max reported properties to be tested
    private static final Integer MAX_PROPERTIES_TO_TEST = 5;

    //Max devices to test
    private static final Integer MAX_DEVICES = 3;

    //Default Page Size for Query
    private static final Integer PAGE_SIZE = 2;

    protected static String iotHubConnectionString = "";
    private static final int INTERTEST_GUARDIAN_DELAY_MILLISECONDS = 2000;

    protected static String publicKeyCert;
    protected static String privateKey;
    protected static String x509Thumbprint;

    // Constants used in for Testing
    private static final String PROPERTY_KEY = "Key";
    private static final String PROPERTY_KEY_QUERY = "KeyQuery";
    private static final String PROPERTY_VALUE = "Value";
    private static final String PROPERTY_VALUE_QUERY = "ValueQuery";
    private static final String PROPERTY_VALUE_UPDATE = "Update";
    private static final String PROPERTY_VALUE_UPDATE2 = "Update2";
    private static final String TAG_KEY = "Tag_Key";
    private static final String TAG_VALUE = "Tag_Value";
    private static final String TAG_VALUE_UPDATE = "Tag_Value_Update";

    // States of SDK
    private static RegistryManager registryManager;
    private static InternalClient internalClient;
    private static RawTwinQuery scRawTwinQueryClient;
    private static DeviceTwin sCDeviceTwin;
    private static DeviceState deviceUnderTest = null;

    private static DeviceState[] devicesUnderTest;

    private DeviceTwinErrorInjectionCommon.DeviceTwinITRunner testInstance;
    private static final long ERROR_INJECTION_WAIT_TIMEOUT = 1 * 60 * 1000; // 1 minute
    private static final long ERROR_INJECTION_EXECUTION_TIMEOUT = 2 * 60 * 1000; // 2 minute

    //How many milliseconds between retry
    private static final Integer RETRY_MILLISECONDS = 100;

    // How much to wait until a message makes it to the server, in milliseconds
    private static final Integer SEND_TIMEOUT_MILLISECONDS = 60000;

    private enum STATUS
    {
        SUCCESS, FAILURE, UNKNOWN
    }

    protected class DeviceTwinStatusCallBack implements IotHubEventCallback
    {
        public void execute(IotHubStatusCode status, Object context)
        {
            DeviceState state = (DeviceState) context;

            //On failure, Don't update status any further
            if ((status == OK || status == OK_EMPTY) && state.deviceTwinStatus != STATUS.FAILURE)
            {
                state.deviceTwinStatus = STATUS.SUCCESS;
            }
            else
            {
                state.deviceTwinStatus = STATUS.FAILURE;
            }
        }
    }

    class DeviceState
    {
        com.microsoft.azure.sdk.iot.service.Device sCDeviceForRegistryManager;
        com.microsoft.azure.sdk.iot.service.Module sCModuleForRegistryManager;
        DeviceTwinDevice sCDeviceForTwin;
        DeviceExtension dCDeviceForTwin;
        OnProperty dCOnProperty = new OnProperty();
        STATUS deviceTwinStatus;
    }

    class PropertyState
    {
        boolean callBackTriggered;
        Property property;
        Object propertyNewValue;
        Integer propertyNewVersion;
    }

    class OnProperty implements TwinPropertyCallBack
    {
        @Override
        public void TwinPropertyCallBack(Property property,  Object context)
        {
            PropertyState propertyState = (PropertyState) context;
            if (property.getKey().equals(propertyState.property.getKey()))
            {
                propertyState.callBackTriggered = true;
                propertyState.propertyNewValue = property.getValue();
                propertyState.propertyNewVersion = property.getVersion();
            }
        }
    }

    class DeviceExtension extends Device
    {
        List<PropertyState> propertyStateList = new LinkedList<>();

        @Override
        public void PropertyCall(String propertyKey, Object propertyValue, Object context)
        {
            PropertyState propertyState = (PropertyState) context;
            if (propertyKey.equals(propertyState.property.getKey()))
            {
                propertyState.callBackTriggered = true;
                propertyState.propertyNewValue = propertyValue;
            }
        }

        synchronized void createNewReportedProperties(int maximumPropertiesToCreate)
        {
            for (int i = 0; i < maximumPropertiesToCreate; i++)
            {
                UUID randomUUID = UUID.randomUUID();
                this.setReportedProp(new Property(PROPERTY_KEY + randomUUID, PROPERTY_VALUE + randomUUID));
            }
        }

        synchronized void updateAllExistingReportedProperties()
        {
            Set<Property> reportedProp = this.getReportedProp();

            for (Property p : reportedProp)
            {
                UUID randomUUID = UUID.randomUUID();
                p.setValue(PROPERTY_VALUE_UPDATE + randomUUID);
            }
        }

        synchronized void updateExistingReportedProperty(int index)
        {
            Set<Property> reportedProp = this.getReportedProp();
            int i = 0;
            for (Property p : reportedProp)
            {
                if (i == index)
                {
                    UUID randomUUID = UUID.randomUUID();
                    p.setValue(PROPERTY_VALUE_UPDATE + randomUUID);
                    break;
                }
                i++;
            }
        }
    }

    private void addMultipleDevices(int numberOfDevices) throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        devicesUnderTest = new DeviceState[numberOfDevices];

        for (int i = 0; i < numberOfDevices; i++)
        {
            devicesUnderTest[i] = new DeviceState();
            String id = "java-device-twin-e2e-test-" + this.testInstance.protocol.toString() + UUID.randomUUID().toString();
            devicesUnderTest[i].sCDeviceForRegistryManager = com.microsoft.azure.sdk.iot.service.Device.createFromId(id, null, null);
            devicesUnderTest[i].sCModuleForRegistryManager = com.microsoft.azure.sdk.iot.service.Module.createFromId(id, "module", null);
            devicesUnderTest[i].sCDeviceForRegistryManager = registryManager.addDevice(devicesUnderTest[i].sCDeviceForRegistryManager);
            devicesUnderTest[i].sCModuleForRegistryManager = registryManager.addModule(devicesUnderTest[i].sCModuleForRegistryManager);
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
            setUpTwin(devicesUnderTest[i]);
        }
    }

    private void removeMultipleDevices(int numberOfDevices) throws IOException, IotHubException, InterruptedException
    {
        for (int i = 0; i < numberOfDevices; i++)
        {
            tearDownTwin(devicesUnderTest[i]);
            registryManager.removeDevice(devicesUnderTest[i].sCDeviceForRegistryManager.getDeviceId());
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        }
    }

    private void setUpTwin(DeviceState deviceState) throws IOException, URISyntaxException, IotHubException, InterruptedException, ModuleClientException
    {
        // set up twin on DeviceClient
        if (internalClient == null)
        {
            deviceState.dCDeviceForTwin = new DeviceExtension();
            if (this.testInstance.authenticationType == SAS)
            {
                if (this.testInstance.moduleId == null)
                {
                    internalClient = new DeviceClient(DeviceConnectionString.get(iotHubConnectionString, deviceState.sCDeviceForRegistryManager),
                            this.testInstance.protocol);
                }
                else
                {
                    internalClient = new ModuleClient(DeviceConnectionString.get(iotHubConnectionString, deviceState.sCDeviceForRegistryManager) + ";ModuleId=" + this.testInstance.moduleId,
                            this.testInstance.protocol);
                }
            }
            else if (this.testInstance.authenticationType == SELF_SIGNED)
            {
                if (this.testInstance.moduleId == null)
                {
                    internalClient = new DeviceClient(DeviceConnectionString.get(iotHubConnectionString, deviceUnderTest.sCDeviceForRegistryManager),
                            this.testInstance.protocol,
                            publicKeyCert,
                            false,
                            privateKey,
                            false);
                }
                else
                {
                    internalClient = new ModuleClient(DeviceConnectionString.get(iotHubConnectionString, deviceState.sCDeviceForRegistryManager) + ";ModuleId=" + this.testInstance.moduleId,
                            this.testInstance.protocol,
                            publicKeyCert,
                            false,
                            privateKey,
                            false);
                }
            }
            IotHubServicesCommon.openClientWithRetry(internalClient);
            if (internalClient instanceof DeviceClient)
            {
                ((DeviceClient) internalClient).startDeviceTwin(new DeviceTwinStatusCallBack(), deviceState, deviceState.dCDeviceForTwin, deviceState);
            }
            else
            {
                ((ModuleClient) internalClient).startTwin(new DeviceTwinStatusCallBack(), deviceState, deviceState.dCDeviceForTwin, deviceState);
            }
            deviceState.deviceTwinStatus = STATUS.UNKNOWN;
        }

        // set up twin on ServiceClient
        if (sCDeviceTwin != null)
        {
            if (testInstance.moduleId == null)
            {
                deviceState.sCDeviceForTwin = new DeviceTwinDevice(deviceState.sCDeviceForRegistryManager.getDeviceId());
            }
            else
            {
                deviceState.sCDeviceForTwin = new DeviceTwinDevice(deviceState.sCDeviceForRegistryManager.getDeviceId(), deviceState.sCModuleForRegistryManager.getId());
            }

            sCDeviceTwin.getTwin(deviceState.sCDeviceForTwin);
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        }
    }

    private static void tearDownTwin(DeviceState deviceState) throws IOException
    {
        // tear down twin on device client
        if (deviceState.sCDeviceForTwin != null)
        {
            deviceState.sCDeviceForTwin.clearTwin();
        }
        if (deviceState.dCDeviceForTwin != null)
        {
            deviceState.dCDeviceForTwin.clean();
        }
        if (internalClient != null)
        {
            internalClient.closeNow();
            internalClient = null;
        }
    }

    public static Collection inputsCommon() throws IOException
    {
        sCDeviceTwin = DeviceTwin.createFromConnectionString(iotHubConnectionString);
        registryManager = RegistryManager.createFromConnectionString(iotHubConnectionString);
        scRawTwinQueryClient = RawTwinQuery.createFromConnectionString(iotHubConnectionString);

        String uuid = UUID.randomUUID().toString();
        String deviceIdAmqps = "java-device-client-e2e-test-amqps".concat("-" + uuid);
        String deviceIdAmqpsWs = "java-device-client-e2e-test-amqpsws".concat("-" + uuid);
        String deviceIdMqtt = "java-device-client-e2e-test-mqtt".concat("-" + uuid);
        String deviceIdMqttWs = "java-device-client-e2e-test-mqttws".concat("-" + uuid);
        String deviceIdMqttX509 = "java-device-client-e2e-test-mqtt-X509".concat("-" + uuid);
        String deviceIdAmqpsX509 = "java-device-client-e2e-test-amqps-X509".concat("-" + uuid);

        String moduleIdAmqps = "java-device-client-e2e-test-amqps-module".concat("-" + uuid);
        String moduleIdAmqpsWs = "java-device-client-e2e-test-amqpsws-module".concat("-" + uuid);
        String moduleIdMqtt = "java-device-client-e2e-test-mqtt-module".concat("-" + uuid);
        String moduleIdMqttWs = "java-device-client-e2e-test-mqttws-module".concat("-" + uuid);

        return Arrays.asList(
            new Object[][]
                {
                    //sas token, device client
                    {deviceIdAmqps, null, AMQPS, SAS, "DeviceClient"},
                    {deviceIdAmqpsWs, null, AMQPS_WS, SAS, "DeviceClient"},
                    {deviceIdMqtt, null, MQTT, SAS, "DeviceClient"},
                    {deviceIdMqttWs,  null, MQTT_WS, SAS, "DeviceClient"},

                    //x509, device client
                    {deviceIdAmqpsX509, null, AMQPS, SELF_SIGNED, "DeviceClient"},
                    {deviceIdMqttX509, null, MQTT, SELF_SIGNED, "DeviceClient"},

                    //sas token, module client
                    {deviceIdAmqps, moduleIdAmqps, AMQPS, SAS, "ModuleClient"},
                    {deviceIdAmqpsWs, moduleIdAmqpsWs, AMQPS_WS, SAS, "ModuleClient"},
                    {deviceIdMqtt, moduleIdMqtt, MQTT, SAS, "ModuleClient"},
                    {deviceIdMqttWs,  moduleIdMqttWs, MQTT_WS, SAS, "ModuleClient"}
               }
        );
    }

    public DeviceTwinErrorInjectionCommon(String deviceId, String moduleId, IotHubClientProtocol protocol, AuthenticationType authenticationType, String clientType)
    {
        this.testInstance = new DeviceTwinErrorInjectionCommon.DeviceTwinITRunner(deviceId, moduleId, protocol, authenticationType, clientType);
    }

    private class DeviceTwinITRunner
    {
        private String deviceId;
        private IotHubClientProtocol protocol;
        private AuthenticationType authenticationType;
        private String moduleId;

        public DeviceTwinITRunner(String deviceId, String moduleId, IotHubClientProtocol protocol, AuthenticationType authenticationType, String clientType)
        {
            this.deviceId = deviceId;
            this.protocol = protocol;
            this.authenticationType = authenticationType;
            this.moduleId = moduleId;
        }
    }

    @Before
    public void setUpNewDeviceAndModule() throws IOException, IotHubException, URISyntaxException, InterruptedException, ModuleClientException
    {
        deviceUnderTest = new DeviceState();
        if (this.testInstance.authenticationType == SAS)
        {
            deviceUnderTest.sCDeviceForRegistryManager = com.microsoft.azure.sdk.iot.service.Device.createFromId(this.testInstance.deviceId, null, null);

            if (this.testInstance.moduleId != null)
            {
                deviceUnderTest.sCModuleForRegistryManager = com.microsoft.azure.sdk.iot.service.Module.createFromId(this.testInstance.deviceId, this.testInstance.moduleId, null);
            }
        }
        else if (this.testInstance.authenticationType == SELF_SIGNED)
        {
            deviceUnderTest.sCDeviceForRegistryManager = com.microsoft.azure.sdk.iot.service.Device.createDevice(this.testInstance.deviceId, SELF_SIGNED);
            deviceUnderTest.sCDeviceForRegistryManager.setThumbprint(x509Thumbprint, x509Thumbprint);
        }
        deviceUnderTest.sCDeviceForRegistryManager = registryManager.addDevice(deviceUnderTest.sCDeviceForRegistryManager);

        if (deviceUnderTest.sCModuleForRegistryManager != null)
        {
            registryManager.addModule(deviceUnderTest.sCModuleForRegistryManager);
        }

        setUpTwin(deviceUnderTest);
    }

    @After
    public void tearDownNewDeviceAndModule() throws IOException, IotHubException
    {
        tearDownTwin(deviceUnderTest);

        registryManager.removeDevice(deviceUnderTest.sCDeviceForRegistryManager.getDeviceId());

        try
        {
            Thread.sleep(INTERTEST_GUARDIAN_DELAY_MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException, IotHubException
    {
        if (registryManager != null)
        {
            registryManager.close();
        }

        registryManager = null;
        sCDeviceTwin = null;
        internalClient = null;
    }

    private void readReportedPropertiesAndVerify(DeviceState deviceState, String startsWithKey, String startsWithValue, int expectedReportedPropCount) throws IOException, IotHubException, InterruptedException
    {
        int actualCount = 0;

        // Check status periodically for success or until timeout
        long startTime = System.currentTimeMillis();
        long timeElapsed;
        while (expectedReportedPropCount != actualCount)
        {
            Thread.sleep(PERIODIC_WAIT_TIME_FOR_VERIFICATION);
            timeElapsed = System.currentTimeMillis() - startTime;
            if (timeElapsed > MAX_WAIT_TIME_FOR_VERIFICATION)
            {
                break;
            }

            actualCount = 0;
            sCDeviceTwin.getTwin(deviceState.sCDeviceForTwin);
            Set<Pair> repProperties = deviceState.sCDeviceForTwin.getReportedProperties();

            for (Pair p : repProperties)
            {
                String val = (String) p.getValue();
                if (p.getKey().startsWith(startsWithKey) && val.startsWith(startsWithValue))
                {
                    actualCount++;
                }
            }
        }
        assertEquals(expectedReportedPropCount, actualCount);
    }

    private void waitAndVerifyTwinStatusBecomesSuccess() throws InterruptedException
    {
        // Check status periodically for success or until timeout
        long startTime = System.currentTimeMillis();
        long timeElapsed = 0;
        while (STATUS.SUCCESS != deviceUnderTest.deviceTwinStatus)
        {
            Thread.sleep(PERIODIC_WAIT_TIME_FOR_VERIFICATION);
            timeElapsed = System.currentTimeMillis() - startTime;
            if (timeElapsed > MAX_WAIT_TIME_FOR_VERIFICATION)
            {
                break;
            }
        }
        assertEquals(STATUS.SUCCESS, deviceUnderTest.deviceTwinStatus);
    }

    private void sendReportedPropertiesAndVerify(int numOfProp) throws IOException, IotHubException, InterruptedException {
        // Act
        // send max_prop RP all at once
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(numOfProp);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());

        // Assert
        waitAndVerifyTwinStatusBecomesSuccess();
        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE, numOfProp);
    }

    void waitAndVerifyDesiredPropertyCallback(String propPrefix, boolean withVersion) throws InterruptedException
    {
        // Check status periodically for success or until timeout
        long startTime = System.currentTimeMillis();
        long timeElapsed = 0;

        for (PropertyState propertyState : deviceUnderTest.dCDeviceForTwin.propertyStateList)
        {
            while (!propertyState.callBackTriggered || !((String) propertyState.propertyNewValue).startsWith(propPrefix))
            {
                Thread.sleep(PERIODIC_WAIT_TIME_FOR_VERIFICATION);
                timeElapsed = System.currentTimeMillis() - startTime;
                if (timeElapsed > MAX_WAIT_TIME_FOR_VERIFICATION)
                {
                    break;
                }
            }
            assertTrue("Callback was not triggered for one or more properties", propertyState.callBackTriggered);
            assertTrue(((String) propertyState.propertyNewValue).startsWith(propPrefix));
            if (withVersion)
            {
                assertNotEquals("Version was not set in the callback", (int) propertyState.propertyNewVersion, -1);
            }
        }
    }

    private void subscribeToDesiredPropertiesAndVerify(int numOfProp) throws IOException, InterruptedException, IotHubException
    {
        // arrange
        for (int i = 0; i < numOfProp; i++)
        {
            PropertyState propertyState = new PropertyState();
            propertyState.callBackTriggered = false;
            propertyState.property = new Property(PROPERTY_KEY + i, PROPERTY_VALUE);
            deviceUnderTest.dCDeviceForTwin.propertyStateList.add(propertyState);
            deviceUnderTest.dCDeviceForTwin.setDesiredPropertyCallback(propertyState.property, deviceUnderTest.dCDeviceForTwin, propertyState);
        }

        // act
        internalClient.subscribeToDesiredProperties(deviceUnderTest.dCDeviceForTwin.getDesiredProp());
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        Set<Pair> desiredProperties = new HashSet<>();
        for (int i = 0; i < numOfProp; i++)
        {
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE_UPDATE + UUID.randomUUID()));
        }
        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, false);
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromTcpConnectionDrop() throws Exception
    {
        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.tcpConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromTcpConnectionDrop() throws Exception
    {
        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.tcpConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromTcpConnectionDrop() throws Exception
    {
        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.tcpConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsConnectionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsConnectionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsConnectionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsConnectionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsSessionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsSessionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsSessionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsSessionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsSessionDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsSessionDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsCBSReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsCBSReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsCBSReqLinkrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsCBSReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsCBSReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsCBSReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsCBSRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsCBSRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsCBSRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsCBSRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsCBSRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.authenticationType != SAS)
        {
            //CBS links are only established when using sas authentication
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsCBSRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsD2CLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsD2CTelemetryLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsD2CDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsD2CTelemetryLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsD2CLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsD2CTelemetryLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsC2DLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. C2D link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsC2DLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromC2DDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. C2D link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsC2DLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsC2DLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. C2D link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }


        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsC2DLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsTwinReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Req link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsTwinReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsTwinReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Req link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsTwinReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsTwinReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Req link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsTwinReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsTwinRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsTwinRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsTwinRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsTwinRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsTwinRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Twin Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsTwinRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsMethodReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsMethodReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsMethodReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Req link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsMethodReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsMethodReqLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Req link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsMethodReqLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void sendReportedPropertiesRecoveredFromAmqpsMethodRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSendReportedPropertiesFlow(ErrorInjectionHelper.amqpsMethodRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void subscribeToDesiredPropertiesRecoveredFromAmqpsMethodRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionSubscribeToDesiredPropertiesFlow(ErrorInjectionHelper.amqpsMethodRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromAmqpsMethodRespLinkDrop() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        if (testInstance.protocol == AMQPS && testInstance.authenticationType == SELF_SIGNED)
        {
            //TODO error injection seems to fail under these circumstances. Method Resp link is never dropped even if waiting a long time
            // Need to talk to service folks about this strange behavior
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsMethodRespLinkDropErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromGracefulShutdownAmqp() throws Exception
    {
        if (!(testInstance.protocol == AMQPS || testInstance.protocol == AMQPS_WS))
        {
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.amqpsGracefulShutdownErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test(timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void getDeviceTwinRecoveredFromGracefulShutdownMqtt() throws Exception
    {
        if (!(testInstance.protocol == MQTT || testInstance.protocol == MQTT_WS))
        {
            return;
        }

        this.errorInjectionGetDeviceTwinFlow(ErrorInjectionHelper.mqttGracefulShutdownErrorInjectionMessage(
                ErrorInjectionHelper.DefaultDelayInSec,
                ErrorInjectionHelper.DefaultDurationInSec));
    }

    @Test (timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void setDesiredPropertiesAtMaxDepthAllowed() throws IOException, IotHubException
    {
        sCDeviceTwin.getTwin(deviceUnderTest.sCDeviceForTwin);

        //Update Twin Tags and Desired Properties
        Set<Pair> desiredProperties = new HashSet<>();

        HashMap<String, String> map5 = new HashMap<>();
        map5.put("5", "this value is at an allowable depth");
        HashMap<String, Map> map4 = new HashMap<>();
        map4.put("4", map5);
        HashMap<String, Map> map3 = new HashMap<>();
        map3.put("3", map4);
        HashMap<String, Map> map2 = new HashMap<>();
        map2.put("2", map3);
        HashMap<String, Map> map1 = new HashMap<>();
        map1.put("1", map2);
        desiredProperties.add(new Pair("0", map1));
        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);

        //This line will ensure that the SDK does not complain when receiving a valid, full twin depth from the service
        sCDeviceTwin.getTwin(deviceUnderTest.sCDeviceForTwin);
    }

    @Test (timeout = ERROR_INJECTION_EXECUTION_TIMEOUT)
    public void setTagsAtMaxDepthAllowed() throws IOException, IotHubException
    {
        sCDeviceTwin.getTwin(deviceUnderTest.sCDeviceForTwin);

        //Update Twin Tags and Desired Properties
        Set<Pair> tags = new HashSet<>();

        HashMap<String, String> map5 = new HashMap<>();
        map5.put("5", "this value is at an allowable depth");
        HashMap<String, Map> map4 = new HashMap<>();
        map4.put("4", map5);
        HashMap<String, Map> map3 = new HashMap<>();
        map3.put("3", map4);
        HashMap<String, Map> map2 = new HashMap<>();
        map2.put("2", map3);
        HashMap<String, Map> map1 = new HashMap<>();
        map1.put("1", map2);
        tags.add(new Pair("0", map1));
        deviceUnderTest.sCDeviceForTwin.setTags(tags);
        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);

        //This line will ensure that the SDK does not complain when receiving a valid, full twin depth from the service
        sCDeviceTwin.getTwin(deviceUnderTest.sCDeviceForTwin);
    }

    private void setDesiredProperties(String queryProperty, String queryPropertyValue, int numberOfDevices) throws IOException, IotHubException
    {
        for (int i = 0; i < numberOfDevices; i++)
        {
            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(queryProperty, queryPropertyValue));
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);

            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }
    }

    private void setConnectionStatusCallBack(final List actualStatusUpdates)
    {
        IotHubConnectionStatusChangeCallback connectionStatusUpdateCallback = new IotHubConnectionStatusChangeCallback()
        {
            @Override
            public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext)
            {
                actualStatusUpdates.add(status);
            }
        };

        this.internalClient.registerConnectionStatusChangeCallback(connectionStatusUpdateCallback, null);
    }

    private void errorInjectionSendReportedPropertiesFlow(Message errorInjectionMessage) throws Exception
    {
        // Arrange
        final List<IotHubConnectionStatus> actualStatusUpdates = new ArrayList<>();
        setConnectionStatusCallBack(actualStatusUpdates);
        sendReportedPropertiesAndVerify(1);

        // Act
        errorInjectionMessage.setExpiryTime(100);
        MessageAndResult errorInjectionMsgAndRet = new MessageAndResult(errorInjectionMessage, null);
        IotHubServicesCommon.sendMessageAndWaitForResponse(internalClient,
                errorInjectionMsgAndRet,
                RETRY_MILLISECONDS,
                SEND_TIMEOUT_MILLISECONDS,
                this.testInstance.protocol);

        // Assert
        IotHubServicesCommon.waitForStabilizedConnection(actualStatusUpdates, ERROR_INJECTION_WAIT_TIMEOUT);
        // add one new reported property
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(1);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());

        waitAndVerifyTwinStatusBecomesSuccess();
        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE, 2);
    }

    private void errorInjectionSubscribeToDesiredPropertiesFlow(Message errorInjectionMessage) throws Exception
    {
        // Arrange
        final List<IotHubConnectionStatus> actualStatusUpdates = new ArrayList<>();
        setConnectionStatusCallBack(actualStatusUpdates);
        subscribeToDesiredPropertiesAndVerify(1);

        // Act
        errorInjectionMessage.setExpiryTime(100);
        MessageAndResult errorInjectionMsgAndRet = new MessageAndResult(errorInjectionMessage, null);
        IotHubServicesCommon.sendMessageAndWaitForResponse(internalClient,
                errorInjectionMsgAndRet,
                RETRY_MILLISECONDS,
                SEND_TIMEOUT_MILLISECONDS,
                this.testInstance.protocol);

        // Assert
        IotHubServicesCommon.waitForStabilizedConnection(actualStatusUpdates, ERROR_INJECTION_WAIT_TIMEOUT);
        deviceUnderTest.dCDeviceForTwin.propertyStateList.get(0).callBackTriggered = false;
        assertEquals(1, deviceUnderTest.sCDeviceForTwin.getDesiredProperties().size());
        Set<Pair> dp = new HashSet<>();
        Pair p = deviceUnderTest.sCDeviceForTwin.getDesiredProperties().iterator().next();
        p.setValue(PROPERTY_VALUE_UPDATE2 + UUID.randomUUID());
        dp.add(p);
        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(dp);

        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);

        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE2, false);
    }

    private void errorInjectionGetDeviceTwinFlow(Message errorInjectionMessage) throws Exception
    {
        // Arrange
        final List<IotHubConnectionStatus> actualStatusUpdates = new ArrayList<>();
        setConnectionStatusCallBack(actualStatusUpdates);
        testGetDeviceTwin();

        // Act
        errorInjectionMessage.setExpiryTime(100);
        MessageAndResult errorInjectionMsgAndRet = new MessageAndResult(errorInjectionMessage, null);
        IotHubServicesCommon.sendMessageAndWaitForResponse(internalClient,
                errorInjectionMsgAndRet,
                RETRY_MILLISECONDS,
                SEND_TIMEOUT_MILLISECONDS,
                this.testInstance.protocol);

        // Assert
        IotHubServicesCommon.waitForStabilizedConnection(actualStatusUpdates, ERROR_INJECTION_WAIT_TIMEOUT);
        for (PropertyState propertyState : deviceUnderTest.dCDeviceForTwin.propertyStateList)
        {
            propertyState.callBackTriggered = false;
            propertyState.propertyNewVersion = -1;
        }

        if (internalClient instanceof DeviceClient)
        {
            ((DeviceClient)internalClient).getDeviceTwin();
        }
        else
        {
            ((ModuleClient)internalClient).getTwin();
        }

        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, true);
    }
}
