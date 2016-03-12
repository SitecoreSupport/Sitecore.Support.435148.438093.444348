using System;
using System.Collections.Generic;
using Sitecore.Data;
using Sitecore.Data.DataProviders;
using Sitecore.Data.Items;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.Templates;
using Sitecore.Diagnostics;
using Sitecore.Data.SqlServer;
using Sitecore.Eventing;
using Sitecore.Support.Data.Eventing;

namespace Sitecore.Support.Data.SqlServer
{
  public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    private readonly SqlDataApi api;

    public SqlServerDataProvider(string connectionString) : base(connectionString)
    {
      this.api = new SqlServerDataApi(connectionString);
    }

    /// <summary>
    /// Gets the data API to the physical database.
    /// </summary>
    /// <value>The data API.</value>
    public new SqlDataApi Api
    {
      get
      {
        return this.api;
      }
    }

    /// <summary>
    /// Gets the event queue driver.
    /// </summary>
    /// <returns>
    /// The event queue driver.
    /// </returns>
    public override EventQueue GetEventQueue()
    {
      return new SqlServerEventQueue(this.Api, this.Database);
    }

    /// <summary>
    /// Saves an item.
    /// </summary>
    /// <param name="itemDefinition">
    /// The item definition.
    /// </param>
    /// <param name="changes">
    /// The changes.
    /// </param>
    /// <param name="context">
    /// The context.
    /// </param>
    /// <returns>
    /// The save item.
    /// </returns>
    public override bool SaveItem(ItemDefinition itemDefinition, ItemChanges changes, CallContext context)
    {
      if (changes.HasPropertiesChanged || changes.HasFieldsChanged)
      {
        Factory.GetRetryer().ExecuteNoResult(() =>
        {
          using (DataProviderTransaction scope = this.Api.CreateTransaction())
          {
            if (changes.HasPropertiesChanged)
            {
              this.UpdateItemDefinition(itemDefinition, changes);
            }

            if (changes.HasFieldsChanged)
            {
              this.UpdateItemFields(itemDefinition.ID, changes);
            }

            scope.Complete();
          }
        });
      }

      RemoveOldBlobs(changes, context);

      this.OnItemSaved(itemDefinition.ID, itemDefinition.TemplateID);

      return true;

    }

    #region Protected Methods
    protected override FieldSharingConverter GetFieldSharingConverter()
    {
      return new Sitecore.Support.Data.DataProviders.Sql.FieldSharingConverter(this);
    }

    /// <summary>
    /// Updates the item definition.
    /// </summary>
    /// <param name="item">
    /// The item to update.
    /// </param>
    /// <param name="changes">
    /// The changes.
    /// </param>
    protected void UpdateItemDefinition(ItemDefinition item, ItemChanges changes)
    {
      string itemName = StringUtil.GetString(changes.GetPropertyValue("name"), item.Name);
      var templateID = MainUtil.GetObject(changes.GetPropertyValue("templateid"), item.TemplateID) as ID;
      var branchId = MainUtil.GetObject(changes.GetPropertyValue("branchid"), item.BranchId) as ID;

      string sql = " UPDATE {0}Items{1}" +
                   " SET {0}Name{1} = {2}name{3}, {0}TemplateID{1} = {2}templateID{3}, {0}MasterID{1} = {2}branchId{3}, {0}Updated{1} = {2}now{3}" +
                   " WHERE {0}ID{1} = {2}itemID{3}";

      this.Api.Execute(
        sql, "itemID", item.ID, "name", itemName, "templateID", templateID, "branchId", branchId, "now", DateTime.Now);
    }

    /// <summary>
    /// Updates the item fields.
    /// </summary>
    /// <param name="itemId">
    /// The item to update.
    /// </param>
    /// <param name="changes">
    /// The changes.
    /// </param>
    protected void UpdateItemFields(ID itemId, ItemChanges changes)
    {
      lock (this.GetLock(itemId))
      {
        DateTime now = DateTime.Now;

        bool fullUpdate = changes.Item.RuntimeSettings.SaveAll;
        if (fullUpdate)
        {
          this.RemoveFields(itemId, changes.Item.Language, changes.Item.Version);
        }

        var updateOrder = new[]
          {
            DefaultFieldSharing.SharingType.Shared, DefaultFieldSharing.SharingType.Unversioned,
            DefaultFieldSharing.SharingType.Versioned
          };

        foreach (DefaultFieldSharing.SharingType sharingType in updateOrder)
        {
          foreach (FieldChange change in changes.FieldChanges)
          {
            if (this.GetSharingType(change) != sharingType)
            {
              continue;
            }

            if (change.RemoveField)
            {
              if (!fullUpdate)
              {
                this.RemoveField(itemId, change);
              }
            }
            else
            {
              switch (sharingType)
              {
                case DefaultFieldSharing.SharingType.Shared:
                  this.WriteSharedField(itemId, change, now, fullUpdate);
                  break;

                case DefaultFieldSharing.SharingType.Unversioned:
                  this.WriteUnversionedField(itemId, change, now, fullUpdate);
                  break;

                case DefaultFieldSharing.SharingType.Versioned:
                  this.WriteVersionedField(itemId, change, now, fullUpdate);
                  break;
              }
            }
          }
        }
      }
    }

    /// <summary>
    /// Gets the type of the sharing.
    /// </summary>
    /// <param name="change">
    /// The change.
    /// </param>
    /// <returns>Sharing type.
    /// </returns>
    protected DefaultFieldSharing.SharingType GetSharingType(FieldChange change)
    {
      TemplateField definition = change.Definition;

      if (definition == null)
      {
        return DefaultFieldSharing.Sharing[change.FieldID];
      }

      return this.GetSharingType(definition);
    }

    /// <summary>
    /// Removes the field.
    /// </summary>
    /// <param name="itemId">
    /// The item id.
    /// </param>
    /// <param name="change">
    /// The change.
    /// </param>
    protected void RemoveField(ID itemId, FieldChange change)
    {
      var sqls = new List<string>();
      DefaultFieldSharing.SharingType sharing = this.GetSharingType(change);

      if (sharing == DefaultFieldSharing.SharingType.Versioned || sharing == DefaultFieldSharing.SharingType.Unknown)
      {
        sqls.Add(
          @"DELETE FROM {0}VersionedFields{1}
                WHERE {0}ItemId{1} = {2}itemId{3}
                AND {0}Version{1} = {2}version{3}
                AND {0}FieldId{1} = {2}fieldId{3}
                AND {0}Language{1} = {2}language{3}");
      }

      if (sharing == DefaultFieldSharing.SharingType.Shared || sharing == DefaultFieldSharing.SharingType.Unknown)
      {
        sqls.Add(
          @" DELETE FROM {0}SharedFields{1}
                 WHERE {0}ItemId{1} = {2}itemId{3}
                 AND {0}FieldId{1} = {2}fieldId{3}");
      }

      if (sharing == DefaultFieldSharing.SharingType.Unversioned || sharing == DefaultFieldSharing.SharingType.Unknown)
      {
        sqls.Add(
          @" DELETE FROM {0}UnversionedFields{1}
                 WHERE {0}ItemId{1} = {2}itemId{3}
                 AND {0}FieldId{1} = {2}fieldId{3}
                 AND {0}Language{1} = {2}language{3}");
      }

      foreach (string sql in sqls)
      {
        this.Api.Execute(
          sql, "itemId", itemId, "fieldId", change.FieldID, "language", change.Language, "version", change.Version);
      }
    }

    /// <summary>
    /// Removes the old blobs.
    /// </summary>
    /// <param name="changes">The changes.</param>
    /// <param name="context">The call context</param>
    private void RemoveOldBlobs(ItemChanges changes, CallContext context)
    {
      foreach (FieldChange change in changes.FieldChanges)
      {
        if (this.CheckIfBlobShouldBeDeleted(change))
        {
          this.RemoveBlobStream(new Guid(change.OriginalValue), context);
        }
      }
    }

    /// <summary>
    /// Checks if BLOB should be deleted.
    /// </summary>
    /// <param name="change">The change.</param>
    /// <returns>
    /// true is the BLOB should be deleted, otherwise false
    /// </returns>
    protected bool CheckIfBlobShouldBeDeleted([NotNull] FieldChange change)
    {
      Assert.ArgumentNotNull(change, "change");

      if (change.IsBlob && change.Value != change.OriginalValue && ID.IsID(change.OriginalValue))
      {
        const string Sql = @"
          IF EXISTS (SELECT NULL {0}Id{1} FROM {0}SharedFields{1} WITH (NOLOCK) WHERE {0}Value{1} LIKE {2}blobId{3})
            BEGIN
              SELECT 1
            END
          ELSE IF EXISTS (SELECT NULL {0}Id{1} FROM {0}VersionedFields{1} WITH (NOLOCK) WHERE {0}Value{1} LIKE {2}blobId{3})
            BEGIN
              SELECT 1
            END
          ELSE IF EXISTS (SELECT NULL {0}FieldId{1} FROM {0}ArchivedFields{1} WITH (NOLOCK) WHERE {0}Value{1} LIKE {2}blobId{3})
            BEGIN
              SELECT 1
            END";

        return !this.Exists(Sql, "blobId", change.OriginalValue);
      }

      return false;
    }

    /// <summary>
    ///   Clears caches when an item is saved.
    /// </summary>
    /// <param name="itemId">
    /// ID of the item
    /// </param>
    /// <param name="templateId">
    /// Template ID of the item.
    /// </param>
    protected void OnItemSaved(ID itemId, ID templateId)
    {
      this.RemovePrefetchDataFromCache(itemId);
      this.ClearLanguageCache(templateId);
    }

    /// <summary>
    /// Clears the language cache, if the template id is a language.
    /// </summary>
    /// <param name="templateId">
    /// The template id.
    /// </param>
    protected void ClearLanguageCache(ID templateId)
    {
      if (templateId == TemplateIDs.Language)
      {
        this.Languages = null;
      }
    }

    #endregion
  }
}
