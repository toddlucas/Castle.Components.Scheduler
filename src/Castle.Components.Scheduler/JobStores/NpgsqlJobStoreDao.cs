using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Npgsql;

namespace Castle.Components.Scheduler.JobStores
{
	/// <summary>
	/// Implements Job Store Dao overrides for PostgreSQL.
	/// </summary>
	public class NpgsqlJobStoreDao : AdoJobStoreDao
	{
		/// <summary>
		/// Creates a PostgreSQL job store.
		/// </summary>
		/// <param name="connectionString">The database connection string</param>
		/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> is null</exception>
		public NpgsqlJobStoreDao(string connectionString)
			: base(connectionString, ":")
		{
		}

		/// <summary>
		/// Creates a PostgreSQL connection.
		/// </summary>
		/// <returns></returns>
		protected override IDbConnection CreateConnection()
		{
			return new NpgsqlConnection(ConnectionString);
		}

		/// <summary>
		/// Returns a string suitable for returning an identity field
		/// in a select statement.
		/// </summary>
		/// <param name="prefix">Prefix for scheduler tables.</param>
		/// <param name="table">Table name of identity column.</param>
		/// <returns>Returns the sequence associated with the specified table.</returns>
		protected override string GetIdentityForTable(string prefix, string table)
		{
			return "CAST(currval('" + prefix + table + "_id_seq') AS INT)";
		}
	}
}
