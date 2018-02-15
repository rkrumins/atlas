/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.eventmanagement.events.v1;

import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.eventmanagement.events.OMRSRegistryEventType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

/**
 * OMRSEventV1RegistrySection describes properties that are used exclusively for registry events.
 */
public class OMRSEventV1RegistrySection implements Serializable
{
    private static final long serialVersionUID = 1L;

    private OMRSRegistryEventType     registryEventType     = null;
    private Date                      registrationTimestamp = null;
    private Connection                remoteConnection      = null;
    private ArrayList<TypeDefSummary> TypeDefList           = null;

    public OMRSEventV1RegistrySection()
    {
    }

    public OMRSRegistryEventType getRegistryEventType()
    {
        return registryEventType;
    }

    public void setRegistryEventType(OMRSRegistryEventType registryEventType)
    {
        this.registryEventType = registryEventType;
    }

    public Date getRegistrationTimestamp()
    {
        return registrationTimestamp;
    }

    public void setRegistrationTimestamp(Date registrationTimestamp)
    {
        this.registrationTimestamp = registrationTimestamp;
    }

    public Connection getRemoteConnection()
    {
        return remoteConnection;
    }

    public void setRemoteConnection(Connection remoteConnection)
    {
        this.remoteConnection = remoteConnection;
    }

    public ArrayList<TypeDefSummary> getTypeDefList()
    {
        return TypeDefList;
    }

    public void setTypeDefList(ArrayList<TypeDefSummary> typeDefList)
    {
        TypeDefList = typeDefList;
    }
}
