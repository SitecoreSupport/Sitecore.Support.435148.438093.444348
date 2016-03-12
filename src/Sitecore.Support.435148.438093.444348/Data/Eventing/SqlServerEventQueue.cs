using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.StringExtensions;

namespace Sitecore.Support.Data.Eventing
{
  /// <summary>
  /// SQL Server EventQueue implementation.
  /// </summary>
  public class SqlServerEventQueue : Sitecore.Data.Eventing.SqlServerEventQueue
  {
    /// <summary>
    /// The NOLOCK table hint also known as READUNCOMMITTED.
    /// </summary>
    private string nolockTableHint;

    public SqlServerEventQueue(SqlDataApi api, Database database)
      : base(api, database)
    {
    }

    /// <summary>
    /// Gets the table hint.
    /// </summary>
    /// <value>
    /// The table hint.
    /// </value>
    protected virtual string TableHint
    {
      get
      {
        if (string.IsNullOrEmpty(this.nolockTableHint))
        {
          string statementBehavior = Settings.GetSetting("EventQueue.SelectStatement.TableHint", "ORIGINAL");
          
          // Don't use WITH when using NOLOCK hint as WITH keyword is deprecated and will be removed in future TSQL releases.
          this.nolockTableHint = (statementBehavior.Equals("original", StringComparison.InvariantCultureIgnoreCase) | statementBehavior.IsNullOrEmpty()) ? string.Empty : statementBehavior;
        }

        return this.nolockTableHint;
      }
    }

    #region IS

    /// <summary>
    /// Returns SqlStatement object with specified hint on the EventQueue table.
    /// </summary>
    /// <param name="query">EventQueueQuery object</param>
    /// <returns></returns>
    protected virtual SqlStatement GetSqlStatementWithHint(EventQueueQuery query)
    {
      Assert.ArgumentNotNull(query, "query");

      SqlStatement statement = new SqlStatement
      {
        Select = "SELECT {0}EventType{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}InstanceName{1}, {0}UserName{1}, {0}Stamp{1}, {0}Created{1}",
        From = "FROM {0}EventQueue{1}" + this.TableHint,
        OrderBy = "ORDER BY {0}Stamp{1}"
      };

      this.AddCriteria(statement, query);

      return statement;
    }

    /// <summary>
    /// Gets the queued events.
    /// </summary>
    /// <param name="query">The query.</param>
    /// <returns>The result.</returns>
    [NotNull]
    public override IEnumerable<QueuedEvent> GetQueuedEvents([NotNull]EventQueueQuery query)

    {
      Assert.ArgumentNotNull(query, "query");

      SqlStatement sqlStatement = this.GetSqlStatementWithHint(query);
      return this.DataApi.CreateObjectReader(sqlStatement.Sql, sqlStatement.GetParameters(), this.CreateQueuedEvent);
    }

    /// <summary>
    /// Performs cleanup of the event queue.
    /// </summary>
    /// <param name="daysToKeep">
    /// Number of days to keep the event queue trail.
    /// </param>
    public override void Cleanup(uint daysToKeep)
    {
      // Overridden to use minutes instead of days.
      // Using daysToKeep as minutes to keep.
      EventQueueQuery query = new EventQueueQuery
      {
        ToUtcDate = DateTime.UtcNow.AddMinutes(-daysToKeep)
      };

      SqlStatement sqlStatement = this.GetSqlStatement(query);
      sqlStatement.Select = "DELETE";
      sqlStatement.OrderBy = string.Empty;

      this.DataApi.Execute(sqlStatement.Sql, sqlStatement.GetParameters());
    }

    #endregion

    #region OBU

    /// <summary>
    /// Returns the top event based on the stamp property.
    /// </summary>
    /// <returns>The most recent QueuedEvent with the queue.</returns>
    public override QueuedEvent GetLastEvent()
    {
      var statement = new SqlStatement
      {
        Select = @"SELECT TOP(1) {0}EventType{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}InstanceName{1}, {0}UserName{1}, {0}Stamp{1}, {0}Created{1}",
        From = @"FROM {0}EventQueue{1}" + this.TableHint,
        OrderBy = @"ORDER BY {0}Stamp{1} DESC"
      };

      return this.DataApi.CreateObjectReader(statement.Sql, new object[0], this.CreateQueuedEvent).FirstOrDefault();
    }

    /// <summary>
    /// Gets the queued event count.
    /// </summary>
    /// <returns>
    /// The result.
    /// </returns>
    public override long GetQueuedEventCount()
    {
      var statement = new SqlStatement
      {
        Select = @"SELECT COUNT(*)",
        From = @"FROM {0}EventQueue{1}" + this.TableHint
      };

      IEnumerable<long> reader = this.DataApi.CreateObjectReader(statement.Sql, new object[0], r => GetLong(r, 0));

      return reader.FirstOrDefault();
    }

    #endregion
  }
}