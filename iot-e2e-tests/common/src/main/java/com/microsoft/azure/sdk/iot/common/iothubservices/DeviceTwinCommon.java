/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.sdk.iot.common.iothubservices;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.sdk.iot.common.*;
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

public class DeviceTwinCommon extends MethodNameLoggingIntegrationTest
{
    // Max time to wait to see it on Hub
    protected static final long PERIODIC_WAIT_TIME_FOR_VERIFICATION = 100; // 0.1 sec
    protected static final long MAX_WAIT_TIME_FOR_VERIFICATION = 60000; // 60 sec
    protected static final long DELAY_BETWEEN_OPERATIONS = 200; // 0.2 sec

    protected static final long MAXIMUM_TIME_FOR_IOTHUB_PROPAGATION_BETWEEN_DEVICE_SERVICE_CLIENTS = DELAY_BETWEEN_OPERATIONS * 10; // 2 sec

    //Max time to wait before timing out test
    protected static final long MAX_MILLISECS_TIMEOUT_KILL_TEST = 120000; // 2 min

    // Max reported properties to be tested
    protected static final Integer MAX_PROPERTIES_TO_TEST = 5;

    //Max devices to test
    protected static final Integer MAX_DEVICES = 3;

    //Default Page Size for Query
    protected static final Integer PAGE_SIZE = 2;

    protected static String iotHubConnectionString = "";
    protected static final int INTERTEST_GUARDIAN_DELAY_MILLISECONDS = 2000;

    protected static String publicKeyCert;
    protected static String privateKey;
    protected static String x509Thumbprint;

    // Constants used in for Testing
    protected static final String PROPERTY_KEY = "Key";
    protected static final String PROPERTY_KEY_QUERY = "KeyQuery";
    protected static final String PROPERTY_VALUE = "Value";
    protected static final String PROPERTY_VALUE_QUERY = "ValueQuery";
    protected static final String PROPERTY_VALUE_UPDATE = "Update";
    protected static final String PROPERTY_VALUE_UPDATE2 = "Update2";
    protected static final String TAG_KEY = "Tag_Key";
    protected static final String TAG_VALUE = "Tag_Value";
    protected static final String TAG_VALUE_UPDATE = "Tag_Value_Update";

    // States of SDK
    protected static RegistryManager registryManager;
    protected static InternalClient internalClient;
    protected static RawTwinQuery scRawTwinQueryClient;
    protected static DeviceTwin sCDeviceTwin;
    protected static DeviceState deviceUnderTest = null;

    protected static DeviceState[] devicesUnderTest;

    protected DeviceTwinCommon.DeviceTwinITRunner testInstance;
    protected static final long ERROR_INJECTION_WAIT_TIMEOUT = 1 * 60 * 1000; // 1 minute
    protected static final long ERROR_INJECTION_EXECUTION_TIMEOUT = 2 * 60 * 1000; // 2 minute

    //How many milliseconds between retry
    protected static final Integer RETRY_MILLISECONDS = 100;

    // How much to wait until a message makes it to the server, in milliseconds
    protected static final Integer SEND_TIMEOUT_MILLISECONDS = 60000;

    protected enum STATUS
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

    protected void addMultipleDevices(int numberOfDevices) throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
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

    protected void removeMultipleDevices(int numberOfDevices) throws IOException, IotHubException, InterruptedException
    {
        for (int i = 0; i < numberOfDevices; i++)
        {
            tearDownTwin(devicesUnderTest[i]);
            registryManager.removeDevice(devicesUnderTest[i].sCDeviceForRegistryManager.getDeviceId());
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        }
    }

    protected void setUpTwin(DeviceState deviceState) throws IOException, URISyntaxException, IotHubException, InterruptedException, ModuleClientException
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

    protected static void tearDownTwin(DeviceState deviceState) throws IOException
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

    public DeviceTwinCommon(String deviceId, String moduleId, IotHubClientProtocol protocol, AuthenticationType authenticationType, String clientType)
    {
        this.testInstance = new DeviceTwinCommon.DeviceTwinITRunner(deviceId, moduleId, protocol, authenticationType, clientType);
    }

    protected class DeviceTwinITRunner
    {
        protected String deviceId;
        protected IotHubClientProtocol protocol;
        protected AuthenticationType authenticationType;
        protected String moduleId;

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

    protected void readReportedPropertiesAndVerify(DeviceState deviceState, String startsWithKey, String startsWithValue, int expectedReportedPropCount) throws IOException, IotHubException, InterruptedException
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

    protected void waitAndVerifyTwinStatusBecomesSuccess() throws InterruptedException
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

    protected void sendReportedPropertiesAndVerify(int numOfProp) throws IOException, IotHubException, InterruptedException {
        // Act
        // send max_prop RP all at once
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(numOfProp);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());

        // Assert
        waitAndVerifyTwinStatusBecomesSuccess();
        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE, numOfProp);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSendReportedProperties() throws IOException, IotHubException, InterruptedException
    {
        sendReportedPropertiesAndVerify(MAX_PROPERTIES_TO_TEST);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSendReportedPropertiesMultiThreaded() throws IOException, IotHubException, InterruptedException
    {
        // arrange
        ExecutorService executor = Executors.newFixedThreadPool(MAX_PROPERTIES_TO_TEST);

        // act
        // send max_prop RP one at a time in parallel
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(1);
                        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
                    }
                    catch (IOException e)
                    {
                        fail(e.getMessage());
                    }
                    assertEquals(deviceUnderTest.deviceTwinStatus, STATUS.SUCCESS);
                }
            });
        }
        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }

        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE, MAX_PROPERTIES_TO_TEST.intValue());
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSendReportedPropertiesSequentially() throws IOException, InterruptedException, IotHubException
    {
        // arrange

        // send max_prop RP one at a time sequentially
        // verify if they are updated by SC
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(1);
            internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
            waitAndVerifyTwinStatusBecomesSuccess();
        }

        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE, MAX_PROPERTIES_TO_TEST.intValue());
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testUpdateReportedProperties() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        // send max_prop RP all at once
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(MAX_PROPERTIES_TO_TEST);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        // act
        // Update RP
        deviceUnderTest.dCDeviceForTwin.updateAllExistingReportedProperties();
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();

        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE_UPDATE, MAX_PROPERTIES_TO_TEST.intValue());
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testUpdateReportedPropertiesMultiThreaded() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        ExecutorService executor = Executors.newFixedThreadPool(MAX_PROPERTIES_TO_TEST);

        // send max_prop RP all at once
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(MAX_PROPERTIES_TO_TEST);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        // act
        // Update RP
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            final int index = i;
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        deviceUnderTest.dCDeviceForTwin.updateExistingReportedProperty(index);
                        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
                        waitAndVerifyTwinStatusBecomesSuccess();
                    }
                    catch (IOException | InterruptedException e)
                    {
                        fail(e.getMessage());
                    }
                }
            });
        }
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }

        // assert
        assertEquals(deviceUnderTest.deviceTwinStatus, STATUS.SUCCESS);

        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE_UPDATE, MAX_PROPERTIES_TO_TEST.intValue());
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testUpdateReportedPropertiesSequential() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        // send max_prop RP all at once
        deviceUnderTest.dCDeviceForTwin.createNewReportedProperties(MAX_PROPERTIES_TO_TEST);
        internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        // act
        // Update RP
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            deviceUnderTest.dCDeviceForTwin.updateExistingReportedProperty(i);
            internalClient.sendReportedProperties(deviceUnderTest.dCDeviceForTwin.getReportedProp());
        }

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();

        // verify if they are received by SC
        readReportedPropertiesAndVerify(deviceUnderTest, PROPERTY_KEY, PROPERTY_VALUE_UPDATE, MAX_PROPERTIES_TO_TEST.intValue());
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

    protected void subscribeToDesiredPropertiesAndVerify(int numOfProp) throws IOException, InterruptedException, IotHubException
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

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSubscribeToDesiredProperties() throws IOException, InterruptedException, IotHubException
    {
        subscribeToDesiredPropertiesAndVerify(MAX_PROPERTIES_TO_TEST);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSubscribeToDesiredPropertiesWithVersion() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        Map<Property, com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair<TwinPropertyCallBack, Object>> desiredPropertiesCB = new HashMap<>();
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            PropertyState propertyState = new PropertyState();
            propertyState.callBackTriggered = false;
            propertyState.propertyNewVersion = -1;
            propertyState.property = new Property(PROPERTY_KEY + i, PROPERTY_VALUE);
            deviceUnderTest.dCDeviceForTwin.propertyStateList.add(propertyState);
            desiredPropertiesCB.put(propertyState.property, new com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair<TwinPropertyCallBack, Object>(deviceUnderTest.dCOnProperty, propertyState));
        }

        // act
        internalClient.subscribeToTwinDesiredProperties(desiredPropertiesCB);
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        Set<Pair> desiredProperties = new HashSet<>();
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE_UPDATE + UUID.randomUUID()));
        }
        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, true);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testGetDeviceTwin() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        Map<Property, com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair<TwinPropertyCallBack, Object>> desiredPropertiesCB = new HashMap<>();
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            PropertyState propertyState = new PropertyState();
            propertyState.property = new Property(PROPERTY_KEY + i, PROPERTY_VALUE);
            deviceUnderTest.dCDeviceForTwin.propertyStateList.add(propertyState);
            desiredPropertiesCB.put(propertyState.property, new com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair<TwinPropertyCallBack, Object>(deviceUnderTest.dCOnProperty, propertyState));
        }
        internalClient.subscribeToTwinDesiredProperties(desiredPropertiesCB);
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        Set<Pair> desiredProperties = new HashSet<>();
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE_UPDATE + UUID.randomUUID()));
        }
        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);
        Thread.sleep(DELAY_BETWEEN_OPERATIONS);

        for (PropertyState propertyState : deviceUnderTest.dCDeviceForTwin.propertyStateList)
        {
            propertyState.callBackTriggered = false;
            propertyState.propertyNewVersion = -1;
        }

        // act
        if (internalClient instanceof DeviceClient)
        {
            ((DeviceClient)internalClient).getDeviceTwin();
        }
        else
        {
            ((ModuleClient)internalClient).getTwin();
        }

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, true);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSubscribeToDesiredPropertiesMultiThreaded() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        ExecutorService executor = Executors.newFixedThreadPool(MAX_PROPERTIES_TO_TEST);

        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
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

        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            final int index = i;
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Set<Pair> desiredProperties = new HashSet<>();
                        desiredProperties.add(new Pair(PROPERTY_KEY + index, PROPERTY_VALUE_UPDATE + UUID.randomUUID()));
                        deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
                        sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);
                    }
                    catch (IotHubException | IOException e)
                    {
                        fail(e.getMessage());
                    }
                }
            });
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        }

        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, false);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testSubscribeToDesiredPropertiesSequentially() throws IOException, InterruptedException, IotHubException
    {
        // arrange
        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
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

        for (int i = 0; i < MAX_PROPERTIES_TO_TEST; i++)
        {
            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE_UPDATE + UUID.randomUUID()));
            deviceUnderTest.sCDeviceForTwin.setDesiredProperties(desiredProperties);
            sCDeviceTwin.updateTwin(deviceUnderTest.sCDeviceForTwin);
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
        }

        // assert
        waitAndVerifyTwinStatusBecomesSuccess();
        waitAndVerifyDesiredPropertyCallback(PROPERTY_VALUE_UPDATE, false);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testAddTagUpdates() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Update tag for multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> tags = new HashSet<>();
            tags.add(new Pair(TAG_KEY + i, TAG_VALUE + i));
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Read updates on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Thread.sleep(DELAY_BETWEEN_OPERATIONS);

            for (Pair t : devicesUnderTest[i].sCDeviceForTwin.getTags())
            {
                assertEquals(t.getKey(), TAG_KEY + i);
                assertEquals(t.getValue(), TAG_VALUE + i);
            }
        }
        removeMultipleDevices(MAX_DEVICES);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testUpdateTagUpdates() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Add tag for multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> tags = new HashSet<>();
            tags.add(new Pair(TAG_KEY + i, TAG_VALUE + i));
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Update Tags on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Set<Pair> tags = devicesUnderTest[i].sCDeviceForTwin.getTags();
            for (Pair tag : tags)
            {
                tag.setValue(TAG_VALUE_UPDATE + i);
            }
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Read updates on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);

            for (Pair t : devicesUnderTest[i].sCDeviceForTwin.getTags())
            {
                assertEquals(t.getKey(), TAG_KEY + i);
                assertEquals(t.getValue(), TAG_VALUE_UPDATE + i);
            }
        }

        // Delete tags
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Set<Pair> tags = devicesUnderTest[i].sCDeviceForTwin.getTags();
            for (Pair tag : tags)
            {
                tag.setValue(null);
            }
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Verify tags were deleted successfully
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);

            assertEquals("Tags were not deleted by being set null", 0, devicesUnderTest[i].sCDeviceForTwin.getTags().size());
        }

        removeMultipleDevices(MAX_DEVICES);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testUpdateDesiredUpdates() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Add desired properties for multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE + i));
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Update desired properties on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Set<Pair> desiredProperties = devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties();
            for (Pair dp : desiredProperties)
            {
                dp.setValue(PROPERTY_VALUE_UPDATE + i);
            }
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Read updates on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);

            for (Pair dp : devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties())
            {
                assertEquals(dp.getKey(), PROPERTY_KEY + i);
                assertEquals(dp.getValue(), PROPERTY_VALUE_UPDATE + i);
            }
            Integer version = devicesUnderTest[i].sCDeviceForTwin.getDesiredPropertiesVersion();
            assertNotNull(version);
        }

        // Remove desired properties
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Set<Pair> desiredProperties = devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties();
            for (Pair dp : desiredProperties)
            {
                dp.setValue(null);
            }
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);
            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Read updates
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);

            assertEquals("Desired properties were not deleted by setting to null", 0, devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties().size());
        }

        removeMultipleDevices(MAX_DEVICES);
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testGetTwinUpdates() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Add tag and desired for multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> tags = new HashSet<>();
            tags.add(new Pair(TAG_KEY + i, TAG_VALUE + i));
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);

            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(PROPERTY_KEY + i, PROPERTY_VALUE + i));
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);

            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Update Tags and desired properties on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);
            Set<Pair> tags = devicesUnderTest[i].sCDeviceForTwin.getTags();
            for (Pair tag : tags)
            {
                tag.setValue(TAG_VALUE_UPDATE + i);
            }
            devicesUnderTest[i].sCDeviceForTwin.setTags(tags);

            Set<Pair> desiredProperties = devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties();
            for (Pair dp : desiredProperties)
            {
                dp.setValue(PROPERTY_VALUE_UPDATE + i);
            }
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);

            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Read updates on multiple devices
        for (int i = 0; i < MAX_DEVICES; i++)
        {
            sCDeviceTwin.getTwin(devicesUnderTest[i].sCDeviceForTwin);

            for (Pair t : devicesUnderTest[i].sCDeviceForTwin.getTags())
            {
                assertEquals(t.getKey(), TAG_KEY + i);
                assertEquals(t.getValue(), TAG_VALUE_UPDATE + i);
            }

            for (Pair dp : devicesUnderTest[i].sCDeviceForTwin.getDesiredProperties())
            {
                assertEquals(dp.getKey(), PROPERTY_KEY + i);
                assertEquals(dp.getValue(), PROPERTY_VALUE_UPDATE + i);
            }
            Integer version = devicesUnderTest[i].sCDeviceForTwin.getDesiredPropertiesVersion();
            assertNotNull(version);
        }
        removeMultipleDevices(MAX_DEVICES);
    }

    @Test
    public void testRawQueryTwin() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();

        // Add same desired on multiple devices
        final String queryProperty = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValue = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        final double actualNumOfDevices = MAX_DEVICES;

        setDesiredProperties(queryProperty, queryPropertyValue, MAX_DEVICES);

        // Raw Query for multiple devices having same property
        final String select = "properties.desired." + queryProperty + " AS " + queryProperty + "," + " COUNT() AS numberOfDevices";
        final String groupBy = "properties.desired." + queryProperty;
        final SqlQuery sqlQuery = SqlQuery.createSqlQuery(select, SqlQuery.FromType.DEVICES, null, groupBy);
        Query rawTwinQuery = scRawTwinQueryClient.query(sqlQuery.getQuery(), PAGE_SIZE);

        while (scRawTwinQueryClient.hasNext(rawTwinQuery))
        {
            String result = scRawTwinQueryClient.next(rawTwinQuery);
            assertNotNull(result);
            Map map = gson.fromJson(result, Map.class);
            if (map.containsKey("numberOfDevices") && map.containsKey(queryProperty))
            {
                double value = (double) map.get("numberOfDevices");
                assertEquals(value, actualNumOfDevices, 0);
            }
        }

        removeMultipleDevices(MAX_DEVICES);
    }

    @Test
    public void testRawQueryMultipleInParallelTwin() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);
        final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Add same desired on multiple devices
        final String queryProperty = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValue = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        final double actualNumOfDevices = MAX_DEVICES;

        final String queryPropertyEven = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValueEven = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        int noOfEvenDevices = 0;

        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(queryProperty, queryPropertyValue));
            if (i % 2 == 0)
            {
                desiredProperties.add(new Pair(queryPropertyEven, queryPropertyValueEven));
                noOfEvenDevices++;
            }
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);

            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // Raw Query for multiple devices having same property
                    final String select = "properties.desired." + queryProperty + " AS " + queryProperty + "," + " COUNT() AS numberOfDevices";
                    final String groupBy = "properties.desired." + queryProperty;
                    final SqlQuery sqlQuery = SqlQuery.createSqlQuery(select, SqlQuery.FromType.DEVICES, null, groupBy);
                    Query rawTwinQuery = scRawTwinQueryClient.query(sqlQuery.getQuery(), PAGE_SIZE);

                    while (scRawTwinQueryClient.hasNext(rawTwinQuery))
                    {
                        String result = scRawTwinQueryClient.next(rawTwinQuery);
                        assertNotNull(result);
                        Map map = gson.fromJson(result, Map.class);
                        if (map.containsKey("numberOfDevices") && map.containsKey(queryProperty))
                        {
                            double value = (double) map.get("numberOfDevices");
                            assertEquals(value, actualNumOfDevices, 0);
                        }
                    }
                }
                catch (Exception e)
                {
                    fail(e.getMessage());
                }

            }
        });

        final double actualNumOfDevicesEven = noOfEvenDevices;
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // Raw Query for multiple devices having same property
                    final String select = "properties.desired." + queryPropertyEven + " AS " + queryPropertyEven + "," + " COUNT() AS numberOfDevices";
                    final String groupBy = "properties.desired." + queryPropertyEven;
                    final SqlQuery sqlQuery = SqlQuery.createSqlQuery(select, SqlQuery.FromType.DEVICES, null, groupBy);
                    Query rawTwinQuery = scRawTwinQueryClient.query(sqlQuery.getQuery(), PAGE_SIZE);

                    while (scRawTwinQueryClient.hasNext(rawTwinQuery))
                    {
                        String result = scRawTwinQueryClient.next(rawTwinQuery);
                        assertNotNull(result);
                        Map map = gson.fromJson(result, Map.class);
                        if (map.containsKey("numberOfDevices") && map.containsKey(queryPropertyEven))
                        {
                            double value = (double) map.get("numberOfDevices");
                            assertEquals(value, actualNumOfDevicesEven, 0);
                        }
                    }
                }
                catch (Exception e)
                {
                    fail(e.getMessage());
                }
            }
        });

        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }

        removeMultipleDevices(MAX_DEVICES);
    }

    @Test
    public void testQueryTwin() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Add same desired on multiple devices
        final String queryProperty = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValue = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();

        setDesiredProperties(queryProperty, queryPropertyValue, MAX_DEVICES);

        // Query multiple devices having same property
        final String where = "is_defined(properties.desired." + queryProperty + ")";
        SqlQuery sqlQuery = SqlQuery.createSqlQuery("*", SqlQuery.FromType.DEVICES, where, null);
        Thread.sleep(MAXIMUM_TIME_FOR_IOTHUB_PROPAGATION_BETWEEN_DEVICE_SERVICE_CLIENTS);
        Query twinQuery = sCDeviceTwin.queryTwin(sqlQuery.getQuery(), PAGE_SIZE);

        for (int i = 0; i < MAX_DEVICES; i++)
        {
            if (sCDeviceTwin.hasNextDeviceTwin(twinQuery))
            {
                DeviceTwinDevice d = sCDeviceTwin.getNextDeviceTwin(twinQuery);
                assertNotNull(d.getVersion());

                for (Pair dp : d.getDesiredProperties())
                {
                    assertEquals(dp.getKey(), queryProperty);
                    assertEquals(dp.getValue(), queryPropertyValue);
                }
            }
        }
        assertFalse(sCDeviceTwin.hasNextDeviceTwin(twinQuery));
        removeMultipleDevices(MAX_DEVICES);
    }

    @Test (timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void testQueryTwinWithContinuationToken() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(PAGE_SIZE + 1);

        // Add same desired on multiple devices so that they can be queried
        final String queryProperty = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValue = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        setDesiredProperties(queryProperty, queryPropertyValue, PAGE_SIZE + 1);

        // Query multiple devices having same property
        final String where = "is_defined(properties.desired." + queryProperty + ")";

        SqlQuery sqlQuery;
        if (this.testInstance.moduleId != null)
        {
            sqlQuery = SqlQuery.createSqlQuery("*", SqlQuery.FromType.MODULES, where, null);
        }
        else
        {
            sqlQuery = SqlQuery.createSqlQuery("*", SqlQuery.FromType.DEVICES, where, null);
        }

        Thread.sleep(MAXIMUM_TIME_FOR_IOTHUB_PROPAGATION_BETWEEN_DEVICE_SERVICE_CLIENTS);
        QueryCollection twinQueryCollection = sCDeviceTwin.queryTwinCollection(sqlQuery.getQuery(), PAGE_SIZE);

        // Run a query and save the continuation token for the second page of results
        QueryCollectionResponse<DeviceTwinDevice> queryCollectionResponse = sCDeviceTwin.next(twinQueryCollection);
        Collection<DeviceTwinDevice> queriedDeviceTwinDeviceCollection = queryCollectionResponse.getCollection();
        String continuationToken = queryCollectionResponse.getContinuationToken();

        // Re-run the same query using the saved continuation token. The results can be predicted since this test caused them
        QueryOptions options = new QueryOptions();
        options.setContinuationToken(continuationToken);
        options.setPageSize(PAGE_SIZE);
        QueryCollection twinQueryToReRun = sCDeviceTwin.queryTwinCollection(sqlQuery.getQuery());
        Collection<DeviceTwinDevice> continuedDeviceTwinDeviceQuery = sCDeviceTwin.next(twinQueryToReRun, options).getCollection();

        // Cleanup
        removeMultipleDevices(PAGE_SIZE + 1);

        // Assert
        assertEquals((long) PAGE_SIZE, queriedDeviceTwinDeviceCollection.size());
        assertEquals(1, continuedDeviceTwinDeviceQuery.size());

        // since order is not guaranteed, we cannot check that the third updated deviceTwinDevice is the third queried.
        // Instead, all we can check is that each updated device twin device is in either the initial query or the continued query.
        ArrayList<String> expectedDeviceIds = new ArrayList<>();
        for (int deviceTwinDeviceIndex = 0; deviceTwinDeviceIndex < PAGE_SIZE + 1; deviceTwinDeviceIndex++)
        {
            expectedDeviceIds.add(devicesUnderTest[deviceTwinDeviceIndex].sCDeviceForTwin.getDeviceId());
        }

        Collection<DeviceTwinDevice> allQueriedDeviceTwinDevices = new ArrayList<>();
        allQueriedDeviceTwinDevices.addAll(continuedDeviceTwinDeviceQuery);
        continuedDeviceTwinDeviceQuery.addAll(queriedDeviceTwinDeviceCollection);

        for (DeviceTwinDevice deviceTwinDevice : allQueriedDeviceTwinDevices)
        {
            if (!expectedDeviceIds.contains(deviceTwinDevice.getDeviceId()))
            {
                fail("Missing deviceTwinDevice: continuation token did not continue query where expected");
            }
        }
    }

    @Test(timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void queryCollectionCanReturnEmptyQueryResults() {
        try
        {
            String fullQuery = "select * from devices where deviceId='nonexistantdevice'";
            DeviceTwin twinClient = DeviceTwin.createFromConnectionString(iotHubConnectionString);
            QueryCollection twinQuery = twinClient.queryTwinCollection(fullQuery);
            QueryOptions options = new QueryOptions();
            QueryCollectionResponse<DeviceTwinDevice> response = twinClient.next(twinQuery, options);

            assertNull(response.getContinuationToken());
            assertTrue(response.getCollection().isEmpty());
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleQueryTwinInParallel() throws IOException, InterruptedException, IotHubException, NoSuchAlgorithmException, URISyntaxException, ModuleClientException
    {
        addMultipleDevices(MAX_DEVICES);

        // Add same desired on multiple devices
        final String queryProperty = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyEven = PROPERTY_KEY_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValue = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        final String queryPropertyValueEven = PROPERTY_VALUE_QUERY + UUID.randomUUID().toString();
        int noOfEvenDevices = 0;
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (int i = 0; i < MAX_DEVICES; i++)
        {
            Set<Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new Pair(queryProperty, queryPropertyValue));
            if (i % 2 == 0)
            {
                desiredProperties.add(new Pair(queryPropertyEven, queryPropertyValueEven));
                noOfEvenDevices++;
            }
            devicesUnderTest[i].sCDeviceForTwin.setDesiredProperties(desiredProperties);

            sCDeviceTwin.updateTwin(devicesUnderTest[i].sCDeviceForTwin);
            devicesUnderTest[i].sCDeviceForTwin.clearTwin();
        }

        // Query multiple devices having same property

        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    final String where = "is_defined(properties.desired." + queryProperty + ")";
                    SqlQuery sqlQuery = SqlQuery.createSqlQuery("*", SqlQuery.FromType.DEVICES, where, null);
                    final Query twinQuery = sCDeviceTwin.queryTwin(sqlQuery.getQuery(), PAGE_SIZE);

                    for (int i = 0; i < MAX_DEVICES; i++)
                    {
                        try
                        {
                            if (sCDeviceTwin.hasNextDeviceTwin(twinQuery))
                            {
                                DeviceTwinDevice d = sCDeviceTwin.getNextDeviceTwin(twinQuery);

                                assertNotNull(d.getVersion());
                                for (Pair dp : d.getDesiredProperties())
                                {
                                    assertEquals(dp.getKey(), queryProperty);
                                    assertEquals(dp.getValue(), queryPropertyValue);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            fail(e.getMessage());
                        }

                        assertFalse(sCDeviceTwin.hasNextDeviceTwin(twinQuery));
                    }
                }
                catch (Exception e)
                {
                    fail(e.getMessage());
                }
            }
        });

        final int maximumEvenDevices = noOfEvenDevices;
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    final String whereEvenDevices = "is_defined(properties.desired." + queryPropertyEven + ")";
                    SqlQuery sqlQueryEvenDevices = SqlQuery.createSqlQuery("*", SqlQuery.FromType.DEVICES, whereEvenDevices, null);
                    final Query twinQueryEven = sCDeviceTwin.queryTwin(sqlQueryEvenDevices.getQuery(), PAGE_SIZE);

                    for (int i = 0; i < maximumEvenDevices; i++)
                    {
                        try
                        {
                            if (sCDeviceTwin.hasNextDeviceTwin(twinQueryEven))
                            {
                                DeviceTwinDevice d = sCDeviceTwin.getNextDeviceTwin(twinQueryEven);

                                assertNotNull(d.getVersion());
                                for (Pair dp : d.getDesiredProperties())
                                {
                                    assertEquals(dp.getKey(), queryPropertyEven);
                                    assertEquals(dp.getValue(), queryPropertyValueEven);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            fail(e.getMessage());
                        }

                        assertFalse(sCDeviceTwin.hasNextDeviceTwin(twinQueryEven));
                    }
                }
                catch (Exception e)
                {
                    fail(e.getMessage());
                }
            }
        });

        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }
        removeMultipleDevices(MAX_DEVICES);
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

    protected void setDesiredProperties(String queryProperty, String queryPropertyValue, int numberOfDevices) throws IOException, IotHubException
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

    protected void setConnectionStatusCallBack(final List actualStatusUpdates)
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

    protected void errorInjectionSendReportedPropertiesFlow(Message errorInjectionMessage) throws Exception
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

    protected void errorInjectionSubscribeToDesiredPropertiesFlow(Message errorInjectionMessage) throws Exception
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

    protected void errorInjectionGetDeviceTwinFlow(Message errorInjectionMessage) throws Exception
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
