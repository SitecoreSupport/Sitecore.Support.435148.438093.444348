using System.Collections.Generic;
using Sitecore.Data;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Globalization;

namespace Sitecore.Support.Data.DataProviders.Sql
{
  public class FieldSharingConverter : Sitecore.Data.DataProviders.Sql.SqlDataProvider.FieldSharingConverter
  {
    private readonly Sitecore.Support.Data.SqlServer.SqlServerDataProvider owner;

    public FieldSharingConverter(Sitecore.Support.Data.SqlServer.SqlServerDataProvider owner)
      : base(owner)
    {
      this.owner = owner;
    }

    /// <summary>
    /// Moves the specified fields from the SharedFields table to the VersionedField one.
    /// </summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <param name="rootitemId">The root item identifier.</param>
    /// <returns><c>true</c> if all fields have been moved, otherwise <c>false</c>.</returns>
    protected override bool MoveDataToVersionedFromShared(ID fieldId, ID rootitemId)
    {
      // Insert an entry into the VersionedFields table from the SharedFields one.
      const string sql = " INSERT INTO {0}VersionedFields{1}({0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1})" +
                         "      SELECT {0}ItemId{1}, {2}language{3}, {2}version{3}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1}" +
                         "        FROM {0}SharedFields{1} sf " +
                         "       WHERE sf.{0}ItemId{1} = {2}itemId{3} AND sf.{0}FieldId{1} = {2}fieldId{3}";

      this.DeleteUnversionedFields(fieldId, rootitemId);
      this.DeleteVersionedFields(fieldId, rootitemId);

      List<ID> itemIdsList = this.ListItemIdsFromSharedFields(fieldId);
      foreach (var itemId in itemIdsList)
      {
        List<Language> languagesList = this.ListLanguagesFromVersionedFields(itemId);
        foreach (var language in languagesList)
        {
          List<Version> versionsList = this.GetVersionsFromVersionedFields(itemId, language);
          foreach (var version in versionsList)
          {
            this.owner.Api.Execute(sql, "language", language, "version", version, "fieldId", fieldId, "itemId", itemId);  
          }
        }
      }

      this.DeleteSharedFields(fieldId, rootitemId);
      
      return true;
    }

    /// <summary>
    /// Moves the specified fields from the SharedFieldsUnversionedFields table to the VersionedField one.
    /// </summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <param name="rootitemId">The root item identifier.</param>
    /// <returns><c>true</c> if all fields have been moved, otherwise <c>false</c>.</returns>
    protected override bool MoveDataToVersionedFromUnversioned(ID fieldId, ID rootitemId)
    {
      // Insert an entry into the VersionedFields table from the UnversionedFields one.
      const string sql = " INSERT INTO {0}VersionedFields{1}({0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1})" +
                         "      SELECT {0}ItemId{1}, {0}Language{1}, {2}version{3}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1}" +
                         "        FROM {0}UnversionedFields{1} uf " +
                         "       WHERE uf.{0}ItemId{1} = {2}itemId{3} AND uf.{0}FieldId{1} = {2}fieldId{3} AND uf.{0}Language{1} = {2}language{3}";

      this.DeleteSharedFields(fieldId, rootitemId);
      this.DeleteVersionedFields(fieldId, rootitemId);

      List<ID> itemIdsList = this.ListItemIdsFromUnversionedFields(fieldId);
      foreach (var itemId in itemIdsList)
      {
        List<Language> languagesList = this.ListLanguagesFromUnversionedFields(itemId);
        foreach (var language in languagesList)
        {
          List<Version> versionsList = this.GetVersionsFromVersionedFields(itemId, language);
          foreach (var version in versionsList)
          {
            this.owner.Api.Execute(sql, "language", language, "version", version, "fieldId", fieldId, "itemId", itemId);
          }
        }
      }
      
      this.DeleteUnversionedFields(fieldId, rootitemId);
      
      return true;
    }

    #region Private Methods

    /// <summary>
    /// Lists the items' IDs from the SharedFields table by field ID.
    /// </summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <returns>
    /// The list of items IDs from the SharedFields table.
    /// </returns>
    private List<ID> ListItemIdsFromSharedFields(ID fieldId)
    {
      var itemIdsList = new List<ID>();

      // Select all items that have the specified field ID.
      string sql = " SELECT {0}ItemId{1}" +
                   "   FROM {0}SharedFields{1} (NOLOCK)" +
                   "  WHERE {0}FieldId{1} = {2}fieldId{3}";

      using (DataProviderReader reader = this.owner.Api.CreateReader(sql, "fieldId", fieldId))
      {
        while (reader.Read())
        {
          itemIdsList.Add(this.owner.Api.GetId(0, reader));
        }
      }

      return itemIdsList;
    }

    /// <summary>
    /// Lists the items' IDs from the UnversionedFields table by field ID.
    /// </summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <returns>
    /// The list of items IDs from the UnversionedFields table.
    /// </returns>
    private List<ID> ListItemIdsFromUnversionedFields(ID fieldId)
    {
      var itemIdsList = new List<ID>();

      // Select all items that have the specified field ID.
      string sql = "   SELECT {0}ItemId{1}" +
                   "     FROM {0}UnversionedFields{1} (NOLOCK)" +
                   "    WHERE {0}FieldId{1} = {2}fieldId{3}" +
                   " GROUP BY {0}ItemId{1}";

      using (DataProviderReader reader = this.owner.Api.CreateReader(sql, "fieldId", fieldId))
      {
        while (reader.Read())
        {
          itemIdsList.Add(this.owner.Api.GetId(0, reader));
        }
      }

      return itemIdsList;
    }

    /// <summary>
    /// Lists the languages from the VersionedFields table by the specified item.
    /// </summary>
    /// <param name="itemId">The item identifier.</param>
    /// <returns>The list of languages for specified item from the VersionedFields table.</returns>
    private List<Language> ListLanguagesFromVersionedFields(ID itemId)
    {
      return this.ListLanguagesFromTable("VersionedFields", itemId);
    }

    /// <summary>
    /// Lists the languages from the UnversionedFields table by the specified item.
    /// </summary>
    /// <param name="itemId">The item identifier.</param>
    /// <returns>The list of languages for specified item from the UnversionedFields table.</returns>
    private List<Language> ListLanguagesFromUnversionedFields(ID itemId)
    {
      return this.ListLanguagesFromTable("UnversionedFields", itemId);
    }

    /// <summary>
    /// Lists the languages from the VersionedField table by the specified item.
    /// </summary>
    /// <param name="tableName">Name of the table.</param>
    /// <param name="itemId">The item identifier.</param>
    /// <returns>The list of languages from the specified table.</returns>
    private List<Language> ListLanguagesFromTable(string tableName, ID itemId)
    {
      var languagesList = new List<Language>();

      // Select all languages that have the specified item ID.
      const string sql = "   SELECT {0}Language{1}" +
                         "     FROM {0}VersionedFields{1} (NOLOCK)" +
                         "    WHERE {0}ItemId{1} = {2}itemId{3}" +
                         " GROUP BY {0}Language{1}";

      using (DataProviderReader reader = this.owner.Api.CreateReader(sql, "itemId", itemId))
      {
        while (reader.Read())
        {
          languagesList.Add(this.owner.Api.GetLanguage(0, reader));
        }
      }

      return languagesList;
    }

    /// <summary>
    /// Gets the versions list for the specified item in the particular language.
    /// </summary>
    /// <param name="itemId">The item identifier.</param>
    /// <param name="language">The item's language.</param>
    /// <returns>The list of versions for the specified item in the particular language from the VersionedFields table</returns>
    private List<Version> GetVersionsFromVersionedFields(ID itemId, Language language)
    {
      var versionsList = new List<Version>();

      // Select all versions that have the specified item ID and language.
      const string sql = "   SELECT {0}Version{1}" +
                         "     FROM {0}VersionedFields{1} (NOLOCK)" +
                         "    WHERE {0}ItemId{1} = {2}itemId{3} AND {0}Language{1} = {2}language{3}" +
                         " GROUP BY {0}Version{1}";

      using (DataProviderReader reader = this.owner.Api.CreateReader(sql, "itemId", itemId, "language", language))
      {
        while (reader.Read())
        {
          versionsList.Add(Version.Parse(this.owner.Api.GetInt(0, reader)));
        }
      }

      return versionsList;
    }

    #endregion
  }
}