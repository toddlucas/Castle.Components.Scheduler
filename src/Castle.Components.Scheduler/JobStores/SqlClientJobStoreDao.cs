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
        /// Creates a PostgreSQL command.
        /// </summary>
        /// <returns></returns>
        protected override IDbCommand CreateCommand()
        {
            return new SqlCommand();
        }

        /// <summary>
        /// Creates a PostgreSQL parameter.
        /// </summary>
        /// <returns></returns>
        protected override IDbDataParameter CreateParameter()
        {
            return new SqlParameter();
        }

        /// <summary>
        /// Creates a PostgreSQL command builder.
        /// </summary>
        /// <returns></returns>
        protected override object CreateCommandBuilder()
        {
            return null; // TODO: implement
        }

        /// <summary>
        /// Returns a string suitable for returning an identity field
        /// in a select statement.
        /// </summary>
        /// <param name="table"></param>
        /// <returns>Returns the scope identity.</returns>
        protected override string GetIdentityForTable(string table)
        {
            // SCOPE_IDENTITY() returns a decimal type by default.
            // All job tables use INT primary keys.
            return "CAST(SCOPE_IDENTITY() AS INT)";
        }
    }
}
