using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace Castle.Components.Scheduler.JobStores
{
	/// <summary>
	/// The ADO DAO base contains standard SQL command text templates.
	/// These templates may be overriden in derived implementations.
	/// </summary>
	public abstract class AdoJobStoreDaoBase
	{
		private const string defaultTablePrefix = "SCHED_";
		private const string defaultClustersTableName = "Clusters";
		private const string defaultSchedulersTableName = "Schedulers";
		private const string defaultJobsTableName = "Jobs";

		/// <summary>
		/// Creates the ADO DAO base.
		/// </summary>
		public AdoJobStoreDaoBase()
		{
			this.TablePrefix = defaultTablePrefix;
			this.ClustersTableName = defaultClustersTableName;
			this.SchedulersTableName = defaultSchedulersTableName;
			this.JobsTableName = defaultJobsTableName;
		}

		/// <summary>
		/// Defines a prefix for the scheduler tables.
		/// </summary>
		public string TablePrefix { get; set; }

		/// <summary>
		/// The cluster table name.
		/// </summary>
		public string ClustersTableName { get; set; }

		/// <summary>
		/// The scheduler table name.
		/// </summary>
		public string SchedulersTableName { get; set; }

		/// <summary>
		/// The job table name.
		/// </summary>
		public string JobsTableName { get; set; }

		/// <summary>
		/// Returns a string suitable for returning an identity field
		/// in a select statement.
		/// </summary>
		/// <param name="prefix">Prefix for scheduler tables.</param>
		/// <param name="table">Table name of identity column.</param>
		/// <returns></returns>
		protected abstract string GetIdentityForTable(string prefix, string table);

#pragma warning disable 1591

		protected virtual string SelectJobCount { get { return stdSelectJobCount; } }
		string stdSelectJobCount =
@"SELECT CAST(COUNT(*) AS INT)
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName";

		protected virtual string SelectVersionedJobCount { get { return stdSelectVersionedJobCount; } }
		string stdSelectVersionedJobCount =
@"SELECT CAST(COUNT(*) AS INT)
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName AND J.Version = @Version";

		protected virtual string SelectJobNames { get { return stdSelectJobNames; } }
		string stdSelectJobNames = "SELECT JobName FROM {jobs}";

		protected virtual string RegisterSchedulerSelectClusterId { get { return stdRegisterSchedulerSelectClusterId; } }
		string stdRegisterSchedulerSelectClusterId = 
@"SELECT ClusterID 
	FROM {clusters} 
	WHERE ClusterName = @ClusterName";

		protected virtual string RegisterSchedulerInsertClusterName { get { return stdRegisterSchedulerInsertClusterName; } }
		string stdRegisterSchedulerInsertClusterName = 
@"INSERT INTO {clusters} 
	(ClusterName) 
VALUES 
	(@ClusterName); 
SELECT {clusters-identity}";

		protected virtual string RegisterSchedulerSelectSchedulerId { get { return stdRegisterSchedulerSelectSchedulerId; } }
		string stdRegisterSchedulerSelectSchedulerId = 
@"SELECT SchedulerID 
	FROM {schedulers} 
	WHERE ClusterID = @ClusterID AND SchedulerGUID = @SchedulerGUID";

		protected virtual string RegisterSchedulerUpdateScheduler { get { return stdRegisterSchedulerUpdateScheduler; } }
		string stdRegisterSchedulerUpdateScheduler = 
@"UPDATE {schedulers} 
	SET SchedulerName = @SchedulerName, LastSeen = @LastSeen 
	WHERE ClusterID = @ClusterID AND SchedulerGUID = @SchedulerGUID";

		protected virtual string RegisterSchedulerInsertScheduler { get { return stdRegisterSchedulerInsertScheduler; } }
		string stdRegisterSchedulerInsertScheduler = 
@"INSERT INTO {schedulers} 
	(ClusterID, SchedulerGUID, SchedulerName, LastSeen) 
VALUES 
	(@ClusterID, @SchedulerGUID, @SchedulerName, @LastSeen)";

		protected virtual string UnregisterSchedulerDeleteScheduler { get { return stdUnregisterSchedulerDeleteScheduler; } }
		string stdUnregisterSchedulerDeleteScheduler = 
@"DELETE
	FROM {schedulers} S
		WHERE S.ClusterID IN (
				SELECT C.ClusterID
					FROM {clusters} C 
						WHERE C.ClusterName = @ClusterName)
			AND S.SchedulerGUID = @SchedulerGUID";

		protected virtual string UnregisterSchedulerUpdateJobs { get { return stdUnregisterSchedulerUpdateJobs; } }
		string stdUnregisterSchedulerUpdateJobs = 
@"UPDATE {jobs}
	SET JobState = @JobState_Orphaned
	WHERE JobState = @JobState_Running
		AND LastExecutionSchedulerGUID = @SchedulerGUID";

		protected virtual string CreateJobSelectClusterId { get { return stdCreateJobSelectClusterId; } }
		string stdCreateJobSelectClusterId =
@"SELECT ClusterID
	FROM {clusters}
	WHERE ClusterName = @ClusterName";

		protected virtual string CreateJobSelectJobId { get { return stdCreateJobSelectJobId; } }
		string stdCreateJobSelectJobId = 
@"SELECT J.JobID AS JobID
	FROM {jobs} J
	WHERE J.ClusterID = @ClusterID AND J.JobName = @JobName";

		protected virtual string CreateJobDeleteJob { get { return stdCreateJobDeleteJob; } }
		string stdCreateJobDeleteJob = "DELETE FROM {jobs} WHERE JobID = @JobID";

		protected virtual string CreateJobUpdateJob { get { return stdCreateJobUpdateJob; } }
		string stdCreateJobUpdateJob = 
@"UPDATE {jobs}
	SET JobDescription = @JobDescription,
		JobKey = @JobKey,
		TriggerObject = @TriggerObject,
		JobDataObject = @JobDataObject,
		JobState = CASE JobState WHEN @JobState_Scheduled THEN @JobState_Pending ELSE JobState END,
		Version = Version + 1
	WHERE JobID = @JobID";

		protected virtual string CreateJobInsertJob { get { return stdCreateJobInsertJob; } }
		string stdCreateJobInsertJob =
@"INSERT INTO {jobs} 
	(ClusterID, JobName, JobDescription, JobKey, TriggerObject, JobDataObject, CreationTime, JobState)
VALUES 
	(@ClusterID, @JobName, @JobDescription, @JobKey, @TriggerObject, @JobDataObject, @CreationTime, @JobState_Pending);
SELECT {jobs-identity}";

		protected virtual string UpdateJobUpdateJob { get { return stdUpdateJobUpdateJob; } }
		string stdUpdateJobUpdateJob = 
@"UPDATE {jobs}
	SET JobName = @UpdatedJobName,
		JobDescription = @UpdatedJobDescription,
		JobKey = @UpdatedJobKey,
		TriggerObject = @UpdatedTriggerObject,
		JobDataObject = @UpdatedJobDataObject,
		JobState = CASE JobState WHEN @JobState_Scheduled THEN @JobState_Pending ELSE JobState END,
		Version = Version + 1
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @ExistingJobName";

		protected virtual string DeleteJobDeleteJob { get { return stdDeleteJobDeleteJob; } }
		string stdDeleteJobDeleteJob = 
@"DELETE
	FROM {jobs} J
		WHERE J.ClusterID IN (
				SELECT C.ClusterID
					FROM {clusters} C 
						WHERE C.ClusterName = @ClusterName)
			AND J.JobName = @JobName";

		protected virtual string GetJobDetailsSelect { get { return stdGetJobDetailsSelect; } }
		string stdGetJobDetailsSelect = 
@"SELECT J.JobName, J.JobDescription, J.JobKey, J.TriggerObject, J.JobDataObject, J.CreationTime,
		J.JobState, J.NextTriggerFireTime, J.NextTriggerMisfireThresholdSeconds,
		J.LastExecutionSchedulerGUID, J.LastExecutionStartTime, J.LastExecutionEndTime, J.LastExecutionSucceeded, J.LastExecutionStatusMessage,
		J.Version
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName";

		protected virtual string SaveJobDetailsUpdateJob { get { return stdSaveJobDetailsUpdateJob; } }
		string stdSaveJobDetailsUpdateJob = 
@"UPDATE {jobs}
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
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName AND J.JobName = @JobName AND J.Version = @Version";

		protected virtual string GetNextJobToProcessTriggerJobs { get { return stdGetNextJobToProcessTriggerJobs; } }
		string stdGetNextJobToProcessTriggerJobs = 
@"UPDATE {jobs}
	SET JobState = @JobState_Triggered
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName
			AND J.JobState = @JobState_Scheduled
			AND (J.NextTriggerFireTime IS NULL OR J.NextTriggerFireTime <= @TimeBasis)";

		protected virtual string GetNextJobToProcessDeleteSchedulers { get { return stdGetNextJobToProcessDeleteSchedulers; } }
		string stdGetNextJobToProcessDeleteSchedulers = 
@"DELETE
	FROM {schedulers} S
		WHERE S.ClusterID IN (
				SELECT C.ClusterID
					FROM {clusters} C 
						WHERE C.ClusterName = @ClusterName)
			AND S.LastSeen < @LapsedExpirationTime";

		protected virtual string GetNextJobToProcessOrphanJobs { get { return stdGetNextJobToProcessOrphanJobs; } }
		string stdGetNextJobToProcessOrphanJobs = 
@"UPDATE {jobs}
	SET JobState = @JobState_Orphaned,
		LastExecutionEndTime = @TimeBasis
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
	LEFT OUTER JOIN {schedulers} S ON S.SchedulerGUID = J.LastExecutionSchedulerGUID
		WHERE C.ClusterName = @ClusterName
			AND J.JobState = @JobState_Running
			AND S.SchedulerID IS NULL";

		protected virtual string GetNextJobToProcessSelectNext { get { return stdGetNextJobToProcessSelectNext; } }
		string stdGetNextJobToProcessSelectNext = 
@"SELECT 
	J.JobName, J.JobDescription, J.JobKey, J.TriggerObject, J.JobDataObject, J.CreationTime,
	J.JobState, J.NextTriggerFireTime, J.NextTriggerMisfireThresholdSeconds,
	J.LastExecutionSchedulerGUID, J.LastExecutionStartTime, J.LastExecutionEndTime, J.LastExecutionSucceeded, J.LastExecutionStatusMessage,
	J.Version
FROM {jobs} J
INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
	WHERE C.ClusterName = @ClusterName
		AND J.JobState IN (@JobState_Pending, @JobState_Triggered, @JobState_Completed, @JobState_Orphaned)";

		protected virtual string GetNextJobToProcessSelectNextTime { get { return stdGetNextJobToProcessSelectNextTime; } }
		string stdGetNextJobToProcessSelectNextTime =
@"SELECT J.NextTriggerFireTime AS NextTriggerFireTime
	FROM {jobs} J
	INNER JOIN {clusters} C ON C.ClusterID = J.ClusterID
		WHERE C.ClusterName = @ClusterName
			AND J.JobState = @JobState_Scheduled
		ORDER BY J.NextTriggerFireTime ASC";

#pragma warning restore 1591

		/// <summary>
		/// Creats a command from a command-text specification containing table name templates.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="text"></param>
		/// <param name="transaction"></param>
		/// <returns></returns>
		protected IDbCommand CreateCommand(IDbConnection connection, IDbTransaction transaction, string text)
		{
			IDbCommand command = connection.CreateCommand();
			if (transaction != null)
				command.Transaction = transaction;

			// TODO: Fix this; it's inefficient.
			command.CommandText = text
				.Replace("{clusters}", TablePrefix + ClustersTableName)
				.Replace("{schedulers}", TablePrefix + SchedulersTableName)
				.Replace("{jobs}", TablePrefix + JobsTableName)
				.Replace("{clusters-identity}", GetIdentityForTable(TablePrefix, ClustersTableName))
				.Replace("{schedulers-identity}", GetIdentityForTable(TablePrefix, SchedulersTableName))
				.Replace("{jobs-identity}", GetIdentityForTable(TablePrefix, JobsTableName));
			return command;
		}

		/// <summary>
		/// Creats a command from a command-text specification containing table name templates.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="text"></param>
		/// <returns></returns>
		protected IDbCommand CreateCommand(IDbConnection connection, string text)
		{
			return CreateCommand(connection, null, text);
		}
	}
}
