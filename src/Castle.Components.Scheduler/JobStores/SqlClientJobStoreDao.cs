using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace Castle.Components.Scheduler.JobStores
{
    /// <summary>
    /// Implements Job Store Dao overrides for PostgreSQL.
    /// </summary>
    public class SqlClientJobStoreDao : AdoJobStoreDao
    {
		/// <summary>
		/// Creates a PostgreSQL job store.
		/// </summary>
		/// <param name="connectionString">The database connection string</param>
		/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> is null</exception>
        public SqlClientJobStoreDao(string connectionString)
			: base(connectionString, "@")
		{
		}

        /// <summary>
        /// Creates a PostgreSQL connection.
        /// </summary>
        /// <returns></returns>
        protected override IDbConnection CreateConnection()
        {
            return new SqlConnection(ConnectionString);
        }

        /// <summary>
        /// Returns a string suitable for returning an identity field
        /// in a select statement.
        /// </summary>
        /// <param name="prefix">Prefix for scheduler tables.</param>
        /// <param name="table">Table name of identity column.</param>
        /// <returns>Returns the scope identity.</returns>
        protected override string GetIdentityForTable(string prefix, string table)
        {
            // SCOPE_IDENTITY() returns a decimal type by default.
            // All job tables use INT primary keys.
            return "CAST(SCOPE_IDENTITY() AS INT)";
        }
    }
}
