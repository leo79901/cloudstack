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

package org.apache.cloudstack.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.cloudstack.api.response.BackupPolicyResponse;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.cloudstack.api.response.VMBackupResponse;
import org.apache.cloudstack.api.response.VMBackupRestorePointResponse;
import org.apache.cloudstack.backup.BackupPolicy;
import org.apache.cloudstack.backup.VMBackup;
import org.apache.cloudstack.context.CallContext;

public abstract class BaseBackupListCmd extends BaseListCmd {

    protected void setupResponseBackupPolicyList(final List<BackupPolicy> policies) {
        final ListResponse<BackupPolicyResponse> response = new ListResponse<>();
        final List<BackupPolicyResponse> responses = new ArrayList<>();
        for (final BackupPolicy policy : policies) {
            if (policy == null) {
                continue;
            }
            BackupPolicyResponse backupPolicyResponse = _responseGenerator.createBackupPolicyResponse(policy);
            responses.add(backupPolicyResponse);
        }
        response.setResponses(responses);
        response.setResponseName(getCommandName());
        setResponseObject(response);
    }

    protected void setupResponseBackupList(final List<VMBackup> backups) {
        final ListResponse<VMBackupResponse> response = new ListResponse<>();
        final List<VMBackupResponse> responses = new ArrayList<>();
        for (VMBackup backup : backups) {
            if (backup == null) {
                continue;
            }
            VMBackupResponse backupResponse = _responseGenerator.createBackupResponse(backup);
            responses.add(backupResponse);
        }
        response.setResponses(responses);
        response.setResponseName(getCommandName());
        setResponseObject(response);
    }

    protected void setupResponseRestorePointsList(final List<VMBackup.RestorePoint> restorePoints) {
        final ListResponse<VMBackupRestorePointResponse> response = new ListResponse<>();
        final List<VMBackupRestorePointResponse> responses = new ArrayList<>();
        for (VMBackup.RestorePoint rp : restorePoints) {
            if (rp == null) {
                continue;
            }
            VMBackupRestorePointResponse rpResponse = new VMBackupRestorePointResponse();
            rpResponse.setId(rp.getId());
            rpResponse.setCreated(rp.getCreated());
            rpResponse.setType(rp.getType());
            rpResponse.setObjectName("vmbackuprestorepoint");
            responses.add(rpResponse);
        }
        response.setResponses(responses);
        response.setResponseName(getCommandName());
        setResponseObject(response);
    }

    @Override
    public long getEntityOwnerId() {
        return CallContext.current().getCallingAccount().getId();
    }
}