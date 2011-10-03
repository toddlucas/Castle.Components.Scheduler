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
	using Core;

	/// <summary>
	/// The Npgsql job store maintains all job state in a PostgreSQL database.
	/// </summary>
	[Singleton]
	public class NpgsqlJobStore : PersistentJobStore
	{
		/// <summary>
		/// Creates a PostgreSQL job store.
		/// </summary>
		/// <param name="connectionString">The database connection string</param>
		/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> is null</exception>
		public NpgsqlJobStore(string connectionString)
			: this(new NpgsqlJobStoreDao(connectionString))
		{
		}

		/// <summary>
		/// Creates a PostgreSQL job store using the specified DAO.
		/// </summary>
		/// <param name="jobStoreDao"></param>
		public NpgsqlJobStore(NpgsqlJobStoreDao jobStoreDao)
			: base(jobStoreDao)
		{
		}

		/// <summary>
		/// Gets the connection string.
		/// </summary>
		public string ConnectionString
		{
			get { return ((NpgsqlJobStoreDao)JobStoreDao).ConnectionString; }
		}
	}
}