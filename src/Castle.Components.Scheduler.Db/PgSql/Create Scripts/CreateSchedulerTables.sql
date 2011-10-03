-- Copyright 2004-2011 Castle Project - http://www.castleproject.org/
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/******************************************************************************
** Purpose : Create Scheduler Tables.
**
** Summary:
**
**   This script creates tables used by Castle.Components.Scheduler for SqlServer.
**   All dates used are in UTC.
**
** Change History:
**
**   Date:    Author:  Bug #    Description:                           
**   -------- -------- ------   -----------------------------------------------
**   05/20/07 Jeff              Initial creation.
**   11/02/11 Todd              Ported to PostgreSQL.
*******************************************************************************
** Copyright (C) 2007-2011 Castle, All Rights Reserved
*******************************************************************************/

DROP TABLE IF EXISTS SCHED_Jobs;

DROP TABLE IF EXISTS SCHED_Schedulers;

DROP TABLE IF EXISTS SCHED_Clusters;

/**
 * Create SCHED_Clusters.
 * Names and identifies all scheduler clusters that exist in the Db.
 * Generally there are only a few cluster rows defined in the Db and they persist
 * for the lifetime of the system.
 */
CREATE SEQUENCE SCHED_Clusters_id_seq;
CREATE TABLE SCHED_Clusters
(
    ClusterID INT DEFAULT nextval('SCHED_Clusters_id_seq') PRIMARY KEY,
    ClusterName VARCHAR(200) UNIQUE NOT NULL
);

/**
 * Create SCHED_Schedulers.
 * Identifies all scheduler instances and the cluster to which
 * they belong in the Db.  One scheduler row is created per active scheduler
 * component (or process) that has a running job.  When the scheduler is terminated
 * its associated records are cleaned up.
 */
CREATE SEQUENCE SCHED_Schedulers_id_seq;
CREATE TABLE SCHED_Schedulers
(
    SchedulerID INT DEFAULT nextval('SCHED_Schedulers_id_seq') PRIMARY KEY,

    ClusterID INT REFERENCES SCHED_Clusters (ClusterID),

    SchedulerGUID UUID NOT NULL UNIQUE,

    SchedulerName VARCHAR(200) NOT NULL,

    LastSeen TIMESTAMP WITH TIME ZONE NOT NULL
);

/**
 * Create SCHED_Jobs.
 * Tracks the status of all scheduled jobs.
 * Running jobs are identified by the (non-null) scheduler instance to which they
 * are associated.
 */
CREATE SEQUENCE SCHED_Jobs_id_seq;
CREATE TABLE SCHED_Jobs
(
    JobID INT DEFAULT nextval('SCHED_Jobs_id_seq') PRIMARY KEY,

    ClusterID INT REFERENCES SCHED_Clusters (ClusterID),

    JobName VARCHAR(200) NOT NULL,

    JobDescription VARCHAR(1000) NOT NULL,

    JobKey VARCHAR(200) NOT NULL,

    TriggerObject BYTEA NULL,
    
    JobDataObject BYTEA NULL,
    
    CreationTime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    JobState INT NOT NULL,
    
    NextTriggerFireTime TIMESTAMP WITH TIME ZONE NULL,

    NextTriggerMisfireThresholdSeconds INT NULL,

    LastExecutionSchedulerGUID UUID NULL,

    LastExecutionStartTime TIMESTAMP WITH TIME ZONE NULL,
    
    LastExecutionEndTime TIMESTAMP WITH TIME ZONE NULL,
    
    LastExecutionSucceeded BOOLEAN NULL,
    
    LastExecutionStatusMessage VARCHAR,
    
    Version INT NOT NULL DEFAULT 0,
    
    UNIQUE(ClusterID, JobName)
);

CREATE INDEX SCHED_Jobs_NextTriggerFireTime_idx ON SCHED_Jobs USING BTREE
(
    NextTriggerFireTime
);

CREATE INDEX SCHED_Jobs_LastExecutionSchedulerGUID_idx ON SCHED_Jobs
(
    LastExecutionSchedulerGUID
);
