// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.cloudstack.vmware;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.cloudstack.api.UpdateVmwareDcCmd;
import org.apache.log4j.Logger;

import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.dao.ClusterDao;
import com.cloud.event.ActionEvent;
import com.cloud.event.EventTypes;
import com.cloud.host.Host;
import com.cloud.host.dao.HostDao;
import com.cloud.host.dao.HostDetailsDao;
import com.cloud.hypervisor.Hypervisor;
import com.cloud.hypervisor.vmware.VmwareDatacenter;
import com.cloud.hypervisor.vmware.VmwareDatacenterVO;
import com.cloud.hypervisor.vmware.VmwareDatacenterZoneMap;
import com.cloud.hypervisor.vmware.dao.VmwareDatacenterDao;
import com.cloud.hypervisor.vmware.dao.VmwareDatacenterZoneMapDao;
import com.cloud.org.Cluster;
import com.cloud.utils.component.ManagerBase;
import com.cloud.utils.db.Transaction;
import com.cloud.utils.db.TransactionCallback;
import com.cloud.utils.db.TransactionStatus;
import com.cloud.utils.exception.CloudRuntimeException;
import com.google.common.base.Strings;

public class VmwareUpdateServiceImpl extends ManagerBase implements VmwareDatacenterUpdateService {
    private static final Logger LOG = Logger.getLogger(VmwareUpdateServiceImpl.class);

    @Inject
    private VmwareDatacenterDao vmwareDcDao;
    @Inject
    private VmwareDatacenterZoneMapDao vmwareDatacenterZoneMapDao;
    @Inject
    private ClusterDao clusterDao;
    @Inject
    private ClusterDetailsDao clusterDetailsDao;
    @Inject
    private HostDao hostDao;
    @Inject
    private HostDetailsDao hostDetailsDao;

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_ZONE_EDIT, eventDescription = "updating VMware datacenter")
    public VmwareDatacenter updateVmwareDatacenter(UpdateVmwareDcCmd cmd) {
        final Long zoneId = cmd.getZoneId();
        final String userName = cmd.getUsername();
        final String password = cmd.getPassword();
        final String vCenterHost = cmd.getVcenter();
        final String vmwareDcName = cmd.getName();
        final Boolean isRecursive = cmd.isRecursive();

        final VmwareDatacenterZoneMap vdcMap = vmwareDatacenterZoneMapDao.findByZoneId(zoneId);
        final VmwareDatacenterVO vmwareDc = vmwareDcDao.findById(vdcMap.getVmwareDcId());
        if (vmwareDc == null) {
            throw new CloudRuntimeException("VMWare datacenter does not exist by provided ID");
        }
        final String oldVCenterHost = vmwareDc.getVcenterHost();

        if (!Strings.isNullOrEmpty(userName)) {
            vmwareDc.setUser(userName);
        }
        if (!Strings.isNullOrEmpty(password)) {
            vmwareDc.setPassword(password);
        }
        if (!Strings.isNullOrEmpty(vCenterHost)) {
            vmwareDc.setVcenterHost(vCenterHost);
        }
        if (!Strings.isNullOrEmpty(vmwareDcName)) {
            vmwareDc.setVmwareDatacenterName(vmwareDcName);
        }
        vmwareDc.setGuid(String.format("%s@%s", vmwareDc.getVmwareDatacenterName(), vmwareDc.getVcenterHost()));

        return Transaction.execute(new TransactionCallback<VmwareDatacenter>() {
            @Override
            public VmwareDatacenter doInTransaction(TransactionStatus status) {
                if (vmwareDcDao.update(vmwareDc.getId(), vmwareDc)) {
                    if (isRecursive) {
                        for (final Cluster cluster : clusterDao.listByDcHyType(zoneId, Hypervisor.HypervisorType.VMware.toString())) {
                            final Map<String, String> clusterDetails = clusterDetailsDao.findDetails(cluster.getId());
                            clusterDetails.put("username", vmwareDc.getUser());
                            clusterDetails.put("password", vmwareDc.getPassword());
                            final String clusterUrl = clusterDetails.get("url");
                            if (!oldVCenterHost.equals(vmwareDc.getVcenterHost()) && !Strings.isNullOrEmpty(clusterUrl)) {
                                clusterDetails.put("url", clusterUrl.replace(oldVCenterHost, vmwareDc.getVcenterHost()));
                            }
                            clusterDetailsDao.persist(cluster.getId(), clusterDetails);
                        }
                        for (final Host host : hostDao.listAll()) {
                            if (host.getDataCenterId() != zoneId || host.getHypervisorType() != Hypervisor.HypervisorType.VMware) {
                                continue;
                            }
                            final Map<String, String> hostDetails = hostDetailsDao.findDetails(host.getId());
                            hostDetails.put("username", vmwareDc.getUser());
                            hostDetails.put("password", vmwareDc.getPassword());
                            final String hostGuid = hostDetails.get("guid");
                            if (!Strings.isNullOrEmpty(hostGuid)) {
                                hostDetails.put("guid", hostGuid.replace(oldVCenterHost, vmwareDc.getVcenterHost()));
                            }
                            hostDetailsDao.persist(host.getId(), hostDetails);
                        }
                    }
                    return vmwareDc;
                }
                return null;
            }
        });
    }

    @Override
    public List<Class<?>> getCommands() {
        return Collections.singletonList(UpdateVmwareDcCmd.class);
    }
}
