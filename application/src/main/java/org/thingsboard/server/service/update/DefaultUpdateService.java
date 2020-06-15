/**
 * Copyright © 2016-2020 The Thingsboard Authors
 *
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
package org.thingsboard.server.service.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.UpdateMessage;
import org.thingsboard.server.common.data.UpgradeBlockersMessage;
import org.thingsboard.server.common.data.exception.ThingsboardErrorCode;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.page.PageDataIterable;
import org.thingsboard.server.common.data.widget.WidgetType;
import org.thingsboard.server.dao.tenant.TenantService;
import org.thingsboard.server.dao.widget.WidgetTypeService;
import org.thingsboard.server.dao.widget.WidgetsBundleService;
import org.thingsboard.server.queue.util.TbCoreComponent;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.thingsboard.server.actors.service.ContextAwareActor.ENTITY_PACK_LIMIT;

@Service
@TbCoreComponent
@Slf4j
public class DefaultUpdateService implements UpdateService {

    private static final String INSTANCE_ID_FILE = ".instance_id";
    private static final String UPDATE_SERVER_BASE_URL = "https://updates.thingsboard.io";

    private static final String PLATFORM_PARAM = "platform";
    private static final String VERSION_PARAM = "version";
    private static final String INSTANCE_ID_PARAM = "instanceId";

    public static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${updates.enabled}")
    private boolean updatesEnabled;

    @Value("${spring.datasource.url}")
    protected String dbUrl;

    @Value("${spring.datasource.username}")
    protected String dbUserName;

    @Value("${spring.datasource.password}")
    protected String dbPassword;

    @Value("${database.entities.type}")
    private String databaseEntitiesType;

    @Value("${database.ts.type}")
    private String databaseTsType;

    @Autowired
    private WidgetTypeService widgetTypeService;

    @Autowired
    private WidgetsBundleService widgetsBundleService;

    @Autowired
    private TenantService tenantService;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, ThingsBoardThreadFactory.forName("tb-update-service"));

    private ScheduledFuture checkUpdatesFuture = null;
    private RestTemplate restClient = new RestTemplate();

    private UpdateMessage updateMessage;

    private String platform;
    private String version;
    private UUID instanceId = null;

    @PostConstruct
    private void init() {
        updateMessage = new UpdateMessage("", false);
        if (updatesEnabled) {
            try {
                platform = System.getProperty("platform", "unknown");
                version = getClass().getPackage().getImplementationVersion();
                if (version == null) {
                    version = "unknown";
                }
                instanceId = parseInstanceId();
                checkUpdatesFuture = scheduler.scheduleAtFixedRate(checkUpdatesRunnable, 0, 1, TimeUnit.HOURS);
            } catch (Exception e) {
                //Do nothing
            }
        }
    }

    private UUID parseInstanceId() throws IOException {
        UUID result = null;
        Path instanceIdPath = Paths.get(INSTANCE_ID_FILE);
        if (instanceIdPath.toFile().exists()) {
            byte[] data = Files.readAllBytes(instanceIdPath);
            if (data != null && data.length > 0) {
                try {
                    result = UUID.fromString(new String(data));
                } catch (IllegalArgumentException e) {
                    //Do nothing
                }
            }
        }
        if (result == null) {
            result = UUID.randomUUID();
            Files.write(instanceIdPath, result.toString().getBytes());
        }
        return result;
    }

    @PreDestroy
    private void destroy() {
        try {
            if (checkUpdatesFuture != null) {
                checkUpdatesFuture.cancel(true);
            }
            scheduler.shutdownNow();
        } catch (Exception e) {
            //Do nothing
        }
    }

    Runnable checkUpdatesRunnable = () -> {
        try {
            log.trace("Executing check update method for instanceId [{}], platform [{}] and version [{}]", instanceId, platform, version);
            ObjectNode request = new ObjectMapper().createObjectNode();
            request.put(PLATFORM_PARAM, platform);
            request.put(VERSION_PARAM, version);
            request.put(INSTANCE_ID_PARAM, instanceId.toString());
            JsonNode response = restClient.postForObject(UPDATE_SERVER_BASE_URL+"/api/thingsboard/updates", request, JsonNode.class);
            updateMessage = new UpdateMessage(
                    response.get("message").asText(),
                    response.get("updateAvailable").asBoolean()
            );
        } catch (Exception e) {
            log.trace(e.getMessage());
        }
    };

    @Override
    public UpdateMessage checkUpdates() {
        return updateMessage;
    }

    @Override
    public UpgradeBlockersMessage getPotentialUpgradeBlockers() throws ThingsboardException {
        return UpgradeBlockersMessage.builder()
                .entitiesDatabaseInfo(getDatabaseInfo("Entities", databaseEntitiesType))
                .tsDatabaseInfo(getDatabaseInfo("TS", databaseTsType))
                .tenantCustomWidgets(getCustomWidgetNamesForTenants())
                .build();
    }

    private String getDatabaseInfo(String dataTypeName, String databaseType) throws ThingsboardException {
        String msgTemplate = dataTypeName + " data is saved in ";
        if ("sql".equals(databaseType)) {
            return msgTemplate + "PostgreSQL, version " + getPostgreSqlVersion();
        }
        return msgTemplate + databaseType;
    }

    private Long getPostgreSqlVersion() throws ThingsboardException {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUserName, dbPassword);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT current_setting('server_version_num')")) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (Exception e) {
            throw new ThingsboardException("Failed to check current PostgreSQL version", ThingsboardErrorCode.GENERAL);
        }
    }

    private Map<String, List<String>> getCustomWidgetNamesForTenants() {
        Map<String, List<String>> customWidgetNamesForTenants = new HashMap<>();
        PageDataIterable<Tenant> tenantIterator = new PageDataIterable<>(tenantService::findTenants, ENTITY_PACK_LIMIT);
        for (Tenant tenant : tenantIterator) {
            List<String> customWidgetNamesForTenant = findCustomWidgetNames(tenant);
            if (!customWidgetNamesForTenant.isEmpty()) {
                customWidgetNamesForTenants.put(tenant.getName(), customWidgetNamesForTenant);
            }
        }
        return customWidgetNamesForTenants;
    }

    private List<String> findCustomWidgetNames(Tenant tenant) {
        return widgetsBundleService.findAllTenantWidgetsBundlesByTenantId(tenant.getId()).stream()
                .filter(widgetsBundle -> !EntityId.NULL_UUID.equals(widgetsBundle.getTenantId().getId()))
                .flatMap(widgetsBundle -> widgetTypeService.findWidgetTypesByTenantIdAndBundleAlias(tenant.getId(),
                        widgetsBundle.getAlias()).stream())
                .map(WidgetType::getName)
                .collect(Collectors.toList());
    }
}
