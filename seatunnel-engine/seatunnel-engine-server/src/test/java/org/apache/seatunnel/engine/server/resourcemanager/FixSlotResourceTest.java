/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class FixSlotResourceTest extends AbstractSeaTunnelServerTest<FixSlotResourceTest> {

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setSlotNum(3);
        seaTunnelConfig.getEngineConfig().setSlotServiceConfig(slotServiceConfig);
        return seaTunnelConfig;
    }

    @Test
    public void testEnoughResource() throws ExecutionException, InterruptedException {
        long jobId = System.currentTimeMillis();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        List<SlotProfile> slotProfiles =
                server.getCoordinatorService()
                        .getResourceManager()
                        .applyResources(jobId, resourceProfiles)
                        .get();
        Assertions.assertEquals(slotProfiles.size(), 3);
        server.getCoordinatorService().getResourceManager().releaseResources(jobId, slotProfiles);
    }

    @Test
    public void testNotEnoughResource() throws ExecutionException, InterruptedException {
        long jobId = System.currentTimeMillis();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        try {
            server.getCoordinatorService()
                    .getResourceManager()
                    .applyResources(jobId, resourceProfiles)
                    .get();
        } catch (ExecutionException e) {
            Assertions.assertTrue(e.getMessage().contains("NoEnoughResourceException"));
        }
        resourceProfiles.remove(0);
        List<SlotProfile> slotProfiles =
                server.getCoordinatorService()
                        .getResourceManager()
                        .applyResources(jobId, resourceProfiles)
                        .get();
        Assertions.assertEquals(slotProfiles.size(), 3);
        server.getCoordinatorService().getResourceManager().releaseResources(jobId, slotProfiles);
    }
}
