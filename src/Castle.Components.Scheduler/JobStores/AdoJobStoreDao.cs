// Copyright 2004-2009 Castle Project - http://www.castleproject.org/
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Castle.Components.Scheduler.JobStores
{
	using System;
	using System.Collections.Generic;
	using System.Data;
	using System.Globalization;
	using Utilities;

	/// <summary>
	/// Abstract base class for an ADO.Net based job store Data Access Object using
	/// ADO commands.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The database schema must be deployed to the database
	/// manually or by some other means before the job store is used.
	/// </para>
	/// </remarks>
	public abstract class AdoJobStoreDao : IJobStoreDao
	{
		private readonly string connectionString;
		private readonly string parameterPrefix;

		private enum CreateJobConflictActionCode
		{
			Ignore = 0,
			Replace = 1,
			Update = 2
		}

		private enum CreateJobResultCode
		{
			JobCreated = 0,
			JobReplaced = 1,
			JobUpdated = 2,
			JobWithSameNameExists = -1
		}

		private enum UpdateJobResultCode
		{
			JobUpdated = 0,
			ExistingJobNotFound = -1,
			JobWithUpdatedNameExists = -2,
		}

        private const string defaultTablePrefix = "SCHED_";

        /// <summary>
        /// Defines a prefix for the scheduler tables.
        /// </summary>
        public string TablePrefix { get; set; }

        /// <summary>
        /// The cluster table name.
        /// </summary>
        public string ClustersTableName { get { return "Clusters"; } }

        /// <summary>
        /// The job table name.
        /// </summary>
        public string JobsTableName { get { return "Jobs"; } }

        /// <summary>
        /// The scheduler table name.
        /// </summary>
        public string SchedulersTableName { get { return "Schedulers"; } }

		/// <summary>
		/// Creates an ADO.Net based job store DAO.
		/// </summary>
		/// <param name="connectionString">The connection string</param>
		/// <param name="parameterPrefix">The stored procedure parameter prefix, if any</param>
		/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> or <paramref name="parameterPrefix"/> is null</exception>
		protected AdoJobStoreDao(string connectionString, string parameterPrefix)
		{
			if (connectionString == null)
				throw new ArgumentNullException("connectionString");
			if (parameterPrefix == null)
				throw new ArgumentNullException("parameterPrefix");

			this.connectionString = connectionString;
			this.parameterPrefix = parameterPrefix;
            this.TablePrefix = defaultTablePrefix;
        }

		/// <summary>
		/// Gets the connection string.
		/// </summary>
		public string ConnectionString
		{
			get { return connectionString; }
		}

        /// <summary>
        /// Creates a database connection.
        /// </summary>
        /// <returns>The database connection</returns>
        protected abstract IDbConnection CreateConnection();

        /// <summary>
        /// Creates a database command object.
        /// </summary>
        /// <returns></returns>
        protected abstract IDbCommand CreateCommand();

        /// <summary>
        /// Creates a database parameter object.
        /// </summary>
        /// <returns></returns>
        protected abstract IDbDataParameter CreateParameter();

        /// <summary>
        /// Creates a command builder.
        /// </summary>
        /// <returns></returns>
        protected abstract object CreateCommandBuilder();

        /// <summary>
        /// Returns a string suitable for returning an identity field
        /// in a select statement.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        protected abstract string GetIdentityForTable(string table);

		/// <summary>
		/// Registers a scheduler.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="schedulerGuid">The scheduler GUID</param>
		/// <param name="schedulerName">The scheduler name, never null</param>
		/// <param name="lastSeenUtc">The time the scheduler was last seen</param>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual void RegisterScheduler(string clusterName, Guid schedulerGuid, string schedulerName,
		                                      DateTime lastSeenUtc)
		{
			try
			{
				using (IDbConnection connection = CreateConnection())
				{
                    connection.Open();

                    //
                    // Create the cluster if needed.
                    //

                    int? clusterId = null;
                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format("SELECT ClusterID FROM {0}{1} WHERE ClusterName = @ClusterName", TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);

                            using (IDataReader rs = command.ExecuteReader())
                            {
                                if (rs.Read())
                                {
                                    int column = rs.GetOrdinal("ClusterID");
                                    if (!rs.IsDBNull(column))
                                    {
                                        clusterId = (int)rs[column];
                                    }
                                }
                            }

                            if (!clusterId.HasValue)
                            {
                                command = connection.CreateCommand();
                                command.Transaction = transaction;
                                command.CommandText = String.Format(
                                    "INSERT INTO {0}{1} (ClusterName) VALUES (@ClusterName); SELECT {2}",
                                    TablePrefix, ClustersTableName, GetIdentityForTable(ClustersTableName));

                                AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                                using (IDataReader rs = command.ExecuteReader())
                                {
                                    if (rs.Read())
                                    {
                                        clusterId = (int)rs[0];
                                    }
                                }
                            }

                            transaction.Commit();
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }

                    //
                    // Create or update the scheduler record.
                    //

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
                                "SELECT SchedulerID FROM {0}{1} WHERE ClusterID = @ClusterID AND SchedulerGUID = @SchedulerGUID", 
                                TablePrefix, SchedulersTableName);

                            AddInputParameter(command, "ClusterID", DbType.String, (int)clusterId);
                            AddInputParameter(command, "SchedulerGUID", DbType.Guid, schedulerGuid);

                            int? schedulerId = null;
                            using (IDataReader rs = command.ExecuteReader())
                            {
                                if (rs.Read())
                                {
                                    int column = rs.GetOrdinal("schedulerID");
                                    if (!rs.IsDBNull(column))
                                    {
                                        schedulerId = (int)rs[column];
                                    }
                                }
                            }

                            if (schedulerId.HasValue)
                            {
                                command = connection.CreateCommand();
                                command.Transaction = transaction;
                                command.CommandText = String.Format(
                                    "UPDATE {0}{1} SET SchedulerName = @SchedulerName, LastSeen = @LastSeen WHERE ClusterID = @ClusterID AND SchedulerGUID = @SchedulerGUID",
                                    TablePrefix, SchedulersTableName);

                                AddInputParameter(command, "SchedulerName", DbType.String, schedulerName);
                                AddInputParameter(command, "LastSeen", DbType.DateTime, lastSeenUtc);
                                AddInputParameter(command, "ClusterID", DbType.String, (int)clusterId);
                                AddInputParameter(command, "SchedulerGUID", DbType.Guid, schedulerGuid);

                                command.ExecuteNonQuery();
                            }
                            else
                            {
                                command = connection.CreateCommand();
                                command.Transaction = transaction;
                                command.CommandText = String.Format(
                                    "INSERT INTO {0}{1} (ClusterID, SchedulerGUID, SchedulerName, LastSeen) VALUES (@ClusterID, @SchedulerGUID, @SchedulerName, @LastSeen)",
                                    TablePrefix, SchedulersTableName);
                                    //"INSERT INTO {0}{1} (ClusterID, SchedulerGUID, SchedulerName, LastSeen) VALUES (@ClusterID, @SchedulerGUID, @SchedulerName, @LastSeen); SELECT {2}",
                                    //TablePrefix, SchedulersTableName, GetIdentityForTable(ClustersTableName));

                                AddInputParameter(command, "ClusterID", DbType.String, (int)clusterId);
                                AddInputParameter(command, "SchedulerGUID", DbType.Guid, schedulerGuid);
                                AddInputParameter(command, "SchedulerName", DbType.String, schedulerName);
                                AddInputParameter(command, "LastSeen", DbType.DateTime, lastSeenUtc);

                                command.ExecuteNonQuery();
                            }

                            transaction.Commit();
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
				}
			}
			catch (Exception ex)
			{
				throw new SchedulerException("The job store was unable to register a scheduler instance in the database.", ex);
			}
		}

		/// <summary>
		/// Unregisters a scheduler and orphans all of its jobs.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="schedulerGuid">The scheduler GUID</param>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual void UnregisterScheduler(string clusterName, Guid schedulerGuid)
		{
			try
			{
				using (IDbConnection connection = CreateConnection())
				{
                    connection.Open();

                    //
                    // Delete the scheduler record.
                    //

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"DELETE S FROM {0}{1} S 
    INNER JOIN {2}{3} C ON C.ClusterID = S.ClusterID
	WHERE C.ClusterName = @ClusterName
	    AND S.SchedulerGUID = @SchedulerGUID",
                                TablePrefix, SchedulersTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "SchedulerGUID", DbType.Guid, schedulerGuid);

                            command.ExecuteNonQuery();

                            //
                            // Immediately orphan all jobs currently on the scheduler.
                            //

                            command.CommandText = String.Format(
@"UPDATE {0}{1}
	SET JobState = @JobState_Orphaned
	WHERE JobState = @JobState_Running
		AND LastExecutionSchedulerGUID = @SchedulerGUID",
                                TablePrefix, JobsTableName);

                            AddInputParameter(command, "JobState_Orphaned", DbType.Int32, (int)JobState.Orphaned);
                            AddInputParameter(command, "JobState_Running", DbType.Int32, (int)JobState.Running);
                            AddInputParameter(command, "SchedulerGUID", DbType.Guid, schedulerGuid);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
				}
			}
			catch (Exception ex)
			{
				throw new SchedulerException("The job store was unable to unregister a scheduler instance in the database.", ex);
			}
		}

		/// <summary>
		/// Creates a job in the database.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="jobSpec">The job specification, never null</param>
		/// <param name="creationTimeUtc">The job creation time</param>
		/// <param name="conflictAction">The action to take if a conflict occurs</param>
		/// <returns>True if the job was created or updated, false if a conflict occurred
		/// and no changes were made</returns>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual bool CreateJob(string clusterName, JobSpec jobSpec, DateTime creationTimeUtc,
		                              CreateJobConflictAction conflictAction)
		{
			try
			{
				using (IDbConnection connection = CreateConnection())
				{
                    connection.Open();
                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            //
                            // Find the cluster.
                            //

                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT ClusterID
	FROM {0}{1}
	WHERE ClusterName = @ClusterName",
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);

                            int? clusterId = null;
                            using (IDataReader rs = command.ExecuteReader())
                            {
                                if (rs.Read())
                                {
                                    clusterId = (int)rs["clusterID"];
                                }
                            }

                            if (!clusterId.HasValue)
                            {
                                throw new SchedulerException("Could not create job because cluster name was not registered.");
                            }

                            //
                            // Find the job if it already exists.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT J.JobID AS JobID
	FROM {0}{1} J
	WHERE J.ClusterID = @ClusterID AND J.JobName = @JobName",
                                TablePrefix, JobsTableName);

                            AddInputParameter(command, "ClusterID", DbType.Int32, clusterId.Value);
                            AddInputParameter(command, "JobName", DbType.String, jobSpec.Name);

                            int? jobId = null;
                            using (IDataReader rs = command.ExecuteReader())
                            {
                                if (rs.Read())
                                {
                                    jobId = (int)rs["JobID"];
                                }
                            }

                            if (jobId.HasValue)
                            {
                                if (conflictAction == CreateJobConflictAction.Ignore)
                                {
                                    transaction.Rollback();
                                    return false;
                                }
                                else if (conflictAction == CreateJobConflictAction.Throw)
                                {
                                    throw new SchedulerException(String.Format(CultureInfo.CurrentCulture,
                                                                               "Job '{0}' already exists.", jobSpec.Name));
                                }
                                else if (conflictAction == CreateJobConflictAction.Replace)
                                {
                                    command = connection.CreateCommand();
                                    command.Transaction = transaction;
                                    command.CommandText = String.Format(
                                        "DELETE FROM {0}{1} WHERE JobID = @JobID",
                                        TablePrefix, JobsTableName);

                                    AddInputParameter(command, "JobID", DbType.Int32, jobId.Value);

                                    command.ExecuteNonQuery();
                                }
                                else if (conflictAction == CreateJobConflictAction.Update)
                                {
                                    command = connection.CreateCommand();
                                    command.Transaction = transaction;
                                    command.CommandText = String.Format(
@"UPDATE {0}{1}
	SET JobDescription = @JobDescription,
		JobKey = @JobKey,
		TriggerObject = @TriggerObject,
		JobDataObject = @JobDataObject,
		JobState = CASE JobState WHEN @JobState_Scheduled THEN @JobState_Pending ELSE JobState END,
		Version = Version + 1
	WHERE JobID = @JobID",
                                        TablePrefix, JobsTableName);

                                    AddInputParameter(command, "JobDescription", DbType.String, jobSpec.Description);
                                    AddInputParameter(command, "JobKey", DbType.String, jobSpec.JobKey);
                                    AddInputParameter(command, "TriggerObject", DbType.Binary,
                                                      DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobSpec.Trigger)));
                                    AddInputParameter(command, "JobDataObject", DbType.Binary,
                                                      DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobSpec.JobData)));

                                    AddInputParameter(command, "JobState_Scheduled", DbType.Int32, (int)JobState.Scheduled);
                                    AddInputParameter(command, "JobState_Pending", DbType.Int32, (int)JobState.Pending);

                                    command.ExecuteNonQuery();

                                    return true;
                                }
                                else
                                {
                                    throw new NotSupportedException("Unexpected conflict action on duplicate job name.");
                                }
                            }

                            //
                            // Insert new job.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"INSERT INTO {0}{1} 
    (ClusterID, JobName, JobDescription, JobKey, TriggerObject, JobDataObject, CreationTime, JobState)
VALUES 
    (@ClusterID, @JobName, @JobDescription, @JobKey, @TriggerObject, @JobDataObject, @CreationTime, @JobState_Pending);
SELECT {2}",
                                TablePrefix, JobsTableName, GetIdentityForTable(JobsTableName));

                            AddInputParameter(command, "JobName", DbType.String, jobSpec.Name);
                            AddInputParameter(command, "JobDescription", DbType.String, jobSpec.Description);
                            AddInputParameter(command, "JobKey", DbType.String, jobSpec.JobKey);
                            AddInputParameter(command, "TriggerObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobSpec.Trigger)));
                            AddInputParameter(command, "JobDataObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobSpec.JobData)));
                            AddInputParameter(command, "CreationTime", DbType.DateTime, creationTimeUtc);

                            AddInputParameter(command, "JobState_Pending", DbType.Int32, (int)JobState.Pending);

                            command.ExecuteNonQuery();

                            transaction.Commit();

                            return true;
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
				}
			}
			catch (Exception ex)
			{
				throw new SchedulerException("The job store was unable to create a job in the database.", ex);
			}
		}

		/// <summary>
		/// Updates an existing job.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="existingJobName">The name of the existing job to update</param>
		/// <param name="updatedJobSpec">The updated job specification</param>
		/// <exception cref="SchedulerException">Thrown if an error occurs or if the job does not exist</exception>
		public virtual void UpdateJob(string clusterName, string existingJobName, JobSpec updatedJobSpec)
		{
            try
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();
                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            //
                            // Look for a job already having the update name.
                            //

                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT COUNT(*) 
    FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName AND J.JobName = @UpdatedJobName",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "UpdatedJobName", DbType.String, updatedJobSpec.Name);

                            int jobCount = (int)command.ExecuteScalar();
                            if (jobCount > 0)
                            {
                                throw new SchedulerException(String.Format(CultureInfo.CurrentCulture,
                                                                           "Cannot rename job '{0}' to '{1}' because another job with the new name already exists.",
                                                                           existingJobName, updatedJobSpec.Name));
                            }

                            //
                            // Look for a job with the existing name.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT COUNT(*) 
    FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName AND J.JobName = @ExistingJobName",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "ExistingJobName", DbType.String, existingJobName);

                            jobCount = (int)command.ExecuteScalar();
                            if (jobCount > 0)
                            {
                                throw new SchedulerException(String.Format(CultureInfo.CurrentCulture,
                                                                           "Job '{0}' does not exist so it cannot be updated.", existingJobName));
                            }

                            //
                            // Update the job.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"UPDATE J
	SET JobName = @UpdatedJobName,
		JobDescription = @UpdatedJobDescription,
		JobKey = @UpdatedJobKey,
		TriggerObject = @UpdatedTriggerObject,
		JobDataObject = @UpdatedJobDataObject,
		JobState = CASE JobState WHEN @JobState_Scheduled THEN @JobState_Pending ELSE JobState END,
		Version = Version + 1
	FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @ExistingJobName",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "UpdatedJobName", DbType.String, updatedJobSpec.Name);
                            AddInputParameter(command, "UpdatedJobDescription", DbType.String, updatedJobSpec.Description);
                            AddInputParameter(command, "UpdatedJobKey", DbType.String, updatedJobSpec.JobKey);
                            AddInputParameter(command, "UpdatedTriggerObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(updatedJobSpec.Trigger)));
                            AddInputParameter(command, "UpdatedJobDataObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(updatedJobSpec.JobData)));

                            AddInputParameter(command, "JobState_Scheduled", DbType.Int32, (int)JobState.Scheduled);
                            AddInputParameter(command, "JobState_Pending", DbType.Int32, (int)JobState.Pending);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "ExistingJobName", DbType.String, existingJobName);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SchedulerException("The job store was unable to update a job in the database.", ex);
            }
		}

		/// <summary>
		/// Deletes the job with the specified name.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="jobName">The job name, never null</param>
		/// <returns>True if a job was actually deleted</returns>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual bool DeleteJob(string clusterName, string jobName)
		{
            try
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();
                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            //
                            // Look for an existing job.
                            //

                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT COUNT(*) 
    FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobName", DbType.String, jobName);

                            int jobCount = (int)command.ExecuteScalar();
                            if (jobCount == 0)
                            {
                                transaction.Commit();
                                return false;
                            }

                            //
                            // Delete the job.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"DELETE J
	FROM {0}{1} J
		INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobName", DbType.String, jobName);

                            command.ExecuteNonQuery();

                            transaction.Commit();

                            return true;
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SchedulerException("The job store was unable to delete a job in the database.", ex);
            }
		}

		/// <summary>
		/// Gets details for the named job.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="jobName">The job name, never null</param>
		/// <returns>The job details, or null if none was found</returns>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual VersionedJobDetails GetJobDetails(string clusterName, string jobName)
		{
            try
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();

                    IDbCommand command = connection.CreateCommand();
                    command.CommandText = String.Format(
@"SELECT J.JobName, J.JobDescription, J.JobKey, J.TriggerObject, J.JobDataObject, J.CreationTime,
	    J.JobState, J.NextTriggerFireTime, J.NextTriggerMisfireThresholdSeconds,
	    J.LastExecutionSchedulerGUID, J.LastExecutionStartTime, J.LastExecutionEndTime, J.LastExecutionSucceeded, J.LastExecutionStatusMessage,
	    J.Version
	FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName",
                        TablePrefix, JobsTableName,
                        TablePrefix, ClustersTableName);

                    AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                    AddInputParameter(command, "JobName", DbType.String, jobName);

                    VersionedJobDetails jobDetails;
                    using (IDataReader reader = command.ExecuteReader())
                    {
						jobDetails = reader.Read() ? BuildJobDetailsFromResultSet(reader) : null;
                    }

					return jobDetails;
                }
            }
            catch (Exception ex)
            {
                throw new SchedulerException("The job store was unable to get job details from the database.", ex);
            }
		}

		/// <summary>
		/// Saves details for the job.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="jobDetails">The job details, never null</param>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual void SaveJobDetails(string clusterName, VersionedJobDetails jobDetails)
		{
            try
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();
                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            //
                            // Look for a job with the existing name and version.
                            //

                            IDbCommand command = connection.CreateCommand();
                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT COUNT(*) 
    FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName AND J.Version = @Version",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobName", DbType.String, jobDetails.JobSpec.Name);
                            AddInputParameter(command, "Version", DbType.Int32, jobDetails.Version);

                            int jobCount = (int)command.ExecuteScalar();
                            if (jobCount == 0)
                            {
                                throw new ConcurrentModificationException(
                                    String.Format("Job '{0}' does not exist or was concurrently modified in the database.",
                                                  jobDetails.JobSpec.Name));
                            }

                            //
                            // Update the job.
                            //

                            command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"UPDATE J
	SET JobDescription = @JobDescription,
		JobKey = @JobKey,
		TriggerObject = @TriggerObject,
		JobDataObject = @JobDataObject,
		JobState = @JobState,
		NextTriggerFireTime = @NextTriggerFireTime,
		NextTriggerMisfireThresholdSeconds = @NextTriggerMisfireThresholdSeconds,
		LastExecutionSchedulerGUID = @LastExecutionSchedulerGUID,
		LastExecutionStartTime = @LastExecutionStartTime,
		LastExecutionEndTime = @LastExecutionEndTime,
		LastExecutionSucceeded = @LastExecutionSucceeded,
		LastExecutionStatusMessage = @LastExecutionStatusMessage,
		Version = @Version + 1
	FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName AND J.Version = @Version",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);

                            AddInputParameter(command, "JobName", DbType.String, jobDetails.JobSpec.Name);
                            AddInputParameter(command, "JobDescription", DbType.String, jobDetails.JobSpec.Description);
                            AddInputParameter(command, "JobKey", DbType.String, jobDetails.JobSpec.JobKey);
                            AddInputParameter(command, "TriggerObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobDetails.JobSpec.Trigger)));

                            AddInputParameter(command, "JobDataObject", DbType.Binary,
                                              DbUtils.MapObjectToDbValue(DbUtils.SerializeObject(jobDetails.JobSpec.JobData)));
                            AddInputParameter(command, "JobState", DbType.Int32, jobDetails.JobState);
                            AddInputParameter(command, "NextTriggerFireTime", DbType.DateTime,
                                              DbUtils.MapNullableToDbValue(jobDetails.NextTriggerFireTimeUtc));
                            int? nextTriggerMisfireThresholdSeconds = jobDetails.NextTriggerMisfireThreshold.HasValue
                                                                        ? (int?)jobDetails.NextTriggerMisfireThreshold.Value.TotalSeconds
                                                                        : null;
                            AddInputParameter(command, "NextTriggerMisfireThresholdSeconds", DbType.Int32,
                                              DbUtils.MapNullableToDbValue(nextTriggerMisfireThresholdSeconds));

                            JobExecutionDetails execution = jobDetails.LastJobExecutionDetails;
                            AddInputParameter(command, "LastExecutionSchedulerGUID", DbType.Guid,
                                              execution != null ? (object)execution.SchedulerGuid : DBNull.Value);
                            AddInputParameter(command, "LastExecutionStartTime", DbType.DateTime,
                                              execution != null ? (object)execution.StartTimeUtc : DBNull.Value);
                            AddInputParameter(command, "LastExecutionEndTime", DbType.DateTime,
                                              execution != null ? DbUtils.MapNullableToDbValue(execution.EndTimeUtc) : DBNull.Value);
                            AddInputParameter(command, "LastExecutionSucceeded", DbType.Boolean,
                                              execution != null ? (object)execution.Succeeded : DBNull.Value);
                            AddInputParameter(command, "LastExecutionStatusMessage", DbType.String,
                                              execution != null ? (object)execution.StatusMessage : DBNull.Value);

                            AddInputParameter(command, "Version", DbType.Int32, jobDetails.Version);

                            command.ExecuteNonQuery();

                            transaction.Commit();

                            jobDetails.Version += 1;
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SchedulerException("The job store was unable to save job details to the database.", ex);
            }
		}

		/// <summary>
		/// Gets the next job to process for the specified scheduler.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <param name="schedulerGuid">The scheduler GUID</param>
		/// <param name="timeBasisUtc">The UTC time to consider as "now"</param>
		/// <param name="nextTriggerFireTimeUtc">Set to the UTC next trigger fire time, or null if there are
		/// no triggers currently scheduled to fire</param>
		/// <param name="schedulerExpirationTimeInSeconds">The scheduler expiration time in seconds, always greater than zero</param>
		/// <returns>The details of job to process or null if none</returns>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public virtual VersionedJobDetails GetNextJobToProcess(string clusterName, Guid schedulerGuid, DateTime timeBasisUtc,
		                                                       int schedulerExpirationTimeInSeconds,
		                                                       out DateTime? nextTriggerFireTimeUtc)
		{
            // Unreferenced: schedulerGuid
            try
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();

                    //
                    // Trigger any scheduled jobs whose time has passed.
                    //

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"UPDATE J
	SET JobState = @JobState_Triggered
    FROM {0}{1} J
    INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName
		    AND J.JobState = @JobState_Scheduled
		    AND (J.NextTriggerFireTime IS NULL OR J.NextTriggerFireTime <= @TimeBasis)",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "JobState_Triggered", DbType.Int32, (int)JobState.Triggered);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobState_Scheduled", DbType.Int32, (int)JobState.Scheduled);
                            AddInputParameter(command, "TimeBasis", DbType.DateTime, timeBasisUtc);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw new SchedulerException("Could not update triggered jobs.", ex);
                        }
                    }

                    //
                    // Purge schedulers that have expired.
                    //

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        DateTime lapsedExpirationTime = timeBasisUtc.AddSeconds(0 - schedulerExpirationTimeInSeconds);

                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"DELETE S
    FROM {0}{1} S
	INNER JOIN {2}{3} C ON C.ClusterID = S.ClusterID
		WHERE C.ClusterName = @ClusterName
			AND S.LastSeen < @LapsedExpirationTime",
                                TablePrefix, SchedulersTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "LapsedExpirationTime", DbType.DateTime, lapsedExpirationTime);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw new SchedulerException("Could not delete expired schedulers.", ex);
                        }
                    }

                    //
                    // Orphan any running jobs whose schedulers have expired.
                    //

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"UPDATE J
	SET JobState = @JobState_Orphaned,
		LastExecutionEndTime = @TimeBasis
    FROM {0}{1} J
	INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	LEFT OUTER JOIN {4}{5} S ON S.SchedulerGUID = J.LastExecutionSchedulerGUID
	    WHERE C.ClusterName = @ClusterName
		    AND J.JobState = @JobState_Running
		    AND S.SchedulerID IS NULL",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName,
                                TablePrefix, SchedulersTableName);

                            AddInputParameter(command, "JobState_Orphaned", DbType.Int32, (int)JobState.Orphaned);
                            AddInputParameter(command, "TimeBasis", DbType.DateTime, timeBasisUtc);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobState_Running", DbType.Int32, (int)JobState.Running);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw new SchedulerException("Could not orphan jobs whose schedulers have expired.", ex);
                        }
                    }

                    //
                    // Get the next job to process.
                    //

                    nextTriggerFireTimeUtc = timeBasisUtc;

                    using (IDbTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            IDbCommand command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = String.Format(
@"SELECT 
    J.JobName, J.JobDescription, J.JobKey, J.TriggerObject, J.JobDataObject, J.CreationTime,
	J.JobState, J.NextTriggerFireTime, J.NextTriggerMisfireThresholdSeconds,
	J.LastExecutionSchedulerGUID, J.LastExecutionStartTime, J.LastExecutionEndTime, J.LastExecutionSucceeded, J.LastExecutionStatusMessage,
	J.Version
FROM {0}{1} J
INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	WHERE C.ClusterName = @ClusterName
		AND J.JobState IN (@JobState_Pending, @JobState_Triggered, @JobState_Completed, @JobState_Orphaned)",
                                TablePrefix, JobsTableName,
                                TablePrefix, ClustersTableName);

                            AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                            AddInputParameter(command, "JobState_Pending", DbType.Int32, (int)JobState.Pending);
                            AddInputParameter(command, "JobState_Triggered", DbType.Int32, (int)JobState.Triggered);
                            AddInputParameter(command, "JobState_Completed", DbType.Int32, (int)JobState.Completed);
                            AddInputParameter(command, "JobState_Orphaned", DbType.Int32, (int)JobState.Orphaned);

                            VersionedJobDetails jobDetails;
                            using (IDataReader reader = command.ExecuteReader())
                            {
                                jobDetails = reader.Read() ? BuildJobDetailsFromResultSet(reader) : null;
                            }

                            if (jobDetails == null)
                            {
                                command = connection.CreateCommand();
                                command.Transaction = transaction;
                                command.CommandText = String.Format(
@"SELECT @NextTriggerFireTime AS NextTriggerFireTime
    FROM {0}{1} J
    INNER JOIN {2}{3} C ON C.ClusterID = J.ClusterID
	    WHERE C.ClusterName = @ClusterName
		    AND J.JobState = @JobState_Scheduled
	    ORDER BY J.NextTriggerFireTime ASC",
                                    TablePrefix, JobsTableName,
                                    TablePrefix, ClustersTableName);

                                AddInputParameter(command, "ClusterName", DbType.String, clusterName);
                                AddInputParameter(command, "JobState_Scheduled", DbType.Int32, (int)JobState.Scheduled);

                                using (IDataReader rs = command.ExecuteReader())
                                {
                                    if (rs.Read())
                                    {
                                        int column = rs.GetOrdinal("NextTriggerFireTime");
                                        if (!rs.IsDBNull(column))
                                        {
                                            nextTriggerFireTimeUtc = (DateTime)rs[column];
                                        }
                                    }
                                }
                            }

                            transaction.Commit();

                            return jobDetails;
                        }
                        catch (Exception)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SchedulerException(
                    "The job store was unable to get job details for the next job to process from the database.", ex);
            }
		}

		/// <summary>
		/// Gets the names of all jobs.
		/// </summary>
		/// <param name="clusterName">The cluster name, never null</param>
		/// <returns>The names of all jobs</returns>
		/// <exception cref="SchedulerException">Thrown if an error occurs</exception>
		public string[] ListJobNames(string clusterName)
		{
			try
			{
				using (IDbConnection connection = CreateConnection())
				{
                    IDbCommand command = connection.CreateCommand();
                    command.CommandText = String.Format("SELECT JobName FROM {0}{1}", TablePrefix, JobsTableName);

                    connection.Open();

					List<string> jobNames = new List<string>();
					using (IDataReader reader = command.ExecuteReader(CommandBehavior.SingleResult))
					{
						while (reader.Read())
							jobNames.Add(reader.GetString(0));
					}

					return jobNames.ToArray();
				}
			}
			catch (Exception ex)
			{
				throw new SchedulerException("The job store was unable to get the list of job names from the database.", ex);
			}
		}

		/// <summary>
		/// Builds a job details object from the result set returned by the spSCHED_GetJobDetails
		/// and spSCHED_GetNextJob stored procedures.
		/// </summary>
		/// <param name="reader">The reader for the result set</param>
		/// <returns>The job details object</returns>
		protected virtual VersionedJobDetails BuildJobDetailsFromResultSet(IDataReader reader)
		{
			string jobName = reader.GetString(0);
			string jobDescription = reader.GetString(1);
			string jobKey = reader.GetString(2);
			Trigger trigger = (Trigger) DbUtils.DeserializeObject(DbUtils.MapDbValueToObject<byte[]>(reader.GetValue(3)));

			JobData jobData = (JobData) DbUtils.DeserializeObject(DbUtils.MapDbValueToObject<byte[]>(reader.GetValue(4)));
			DateTime creationTimeUtc = DateTimeUtils.AssumeUniversalTime(reader.GetDateTime(5));
			JobState jobState = (JobState) reader.GetInt32(6);
			DateTime? nextTriggerFireTimeUtc =
				DateTimeUtils.AssumeUniversalTime(DbUtils.MapDbValueToNullable<DateTime>(reader.GetValue(7)));
			int? nextTriggerMisfireThresholdSeconds = DbUtils.MapDbValueToNullable<int>(reader.GetValue(8));
			TimeSpan? nextTriggerMisfireThreshold = nextTriggerMisfireThresholdSeconds.HasValue
			                                        	?
			                                        		new TimeSpan(0, 0, nextTriggerMisfireThresholdSeconds.Value)
			                                        	: (TimeSpan?) null;

			Guid? lastExecutionSchedulerGuid = DbUtils.MapDbValueToNullable<Guid>(reader.GetValue(9));
			DateTime? lastExecutionStartTimeUtc =
				DateTimeUtils.AssumeUniversalTime(DbUtils.MapDbValueToNullable<DateTime>(reader.GetValue(10)));
			DateTime? lastExecutionEndTimeUtc =
				DateTimeUtils.AssumeUniversalTime(DbUtils.MapDbValueToNullable<DateTime>(reader.GetValue(11)));
			bool? lastExecutionSucceeded = DbUtils.MapDbValueToNullable<bool>(reader.GetValue(12));
			string lastExecutionStatusMessage = DbUtils.MapDbValueToObject<string>(reader.GetValue(13));

			int version = reader.GetInt32(14);

			JobSpec jobSpec = new JobSpec(jobName, jobDescription, jobKey, trigger);
			jobSpec.JobData = jobData;

			VersionedJobDetails details = new VersionedJobDetails(jobSpec, creationTimeUtc, version);
			details.JobState = jobState;
			details.NextTriggerFireTimeUtc = nextTriggerFireTimeUtc;
			details.NextTriggerMisfireThreshold = nextTriggerMisfireThreshold;

			if (lastExecutionSchedulerGuid.HasValue && lastExecutionStartTimeUtc.HasValue)
			{
				JobExecutionDetails execution = new JobExecutionDetails(lastExecutionSchedulerGuid.Value,
				                                                        lastExecutionStartTimeUtc.Value);
				execution.EndTimeUtc = lastExecutionEndTimeUtc;
				execution.Succeeded = lastExecutionSucceeded.GetValueOrDefault();
				execution.StatusMessage = lastExecutionStatusMessage == null ? "" : lastExecutionStatusMessage;

				details.LastJobExecutionDetails = execution;
			}

			return details;
		}

		/// <summary>
		/// Creates a command to invoke the specified stored procedure.
		/// </summary>
		/// <param name="connection">The Db connection</param>
		/// <param name="spName">The stored procedure name</param>
		/// <returns>The Db command</returns>
		protected virtual IDbCommand CreateStoredProcedureCommand(IDbConnection connection, string spName)
		{
			IDbCommand command = connection.CreateCommand();
			command.CommandType = CommandType.StoredProcedure;
			command.CommandText = spName;
			return command;
		}

        /// <summary>
		/// Creates a generic parameter and adds it to a command.
		/// </summary>
		/// <param name="command">The command</param>
		/// <param name="name">The parameter name</param>
		/// <param name="type">The parameter value type</param>
		/// <returns>The parameter</returns>
		protected virtual IDbDataParameter AddParameter(IDbCommand command, string name, DbType type)
		{
			IDbDataParameter parameter = command.CreateParameter();
			parameter.ParameterName = parameterPrefix + name;
			parameter.DbType = type;

			command.Parameters.Add(parameter);
			return parameter;
		}

		/// <summary>
		/// Creates an input parameter and adds it to a command.
		/// </summary>
		/// <param name="command">The command</param>
		/// <param name="name">The parameter name</param>
		/// <param name="type">The parameter value type</param>
		/// <param name="value">The value of the parameter</param>
		/// <returns>The parameter</returns>
		protected IDbDataParameter AddInputParameter(IDbCommand command, string name, DbType type, object value)
		{
			IDbDataParameter parameter = AddParameter(command, name, type);
			parameter.Direction = ParameterDirection.Input;
			parameter.Value = value;
			return parameter;
		}

		/// <summary>
		/// Creates an output parameter and adds it to a command.
		/// </summary>
		/// <param name="command">The command</param>
		/// <param name="name">The parameter name</param>
		/// <param name="type">The parameter value type</param>
		/// <returns>The parameter</returns>
		protected IDbDataParameter AddOutputParameter(IDbCommand command, string name, DbType type)
		{
			IDbDataParameter parameter = AddParameter(command, name, type);
			parameter.Direction = ParameterDirection.Output;
			return parameter;
		}
	}
}