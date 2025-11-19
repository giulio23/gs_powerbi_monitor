codeunit 90133 "Power BI Dataset Manager"
{
    // Dataset-specific operations for Power BI Monitor
    Permissions = tabledata "Power BI Dataset" = RIMD;

    var
        PowerBIHttpClient: Codeunit "Power BI Http Client";
        PowerBIJsonProcessor: Codeunit "Power BI Json Processor";

    /// <summary>
    /// Synchronizes datasets for a specific workspace
    /// </summary>
    /// <param name="WorkspaceId">The workspace ID to synchronize</param>
    /// <returns>True if synchronization was successful</returns>
    procedure SynchronizeDatasets(WorkspaceId: Guid): Boolean
    var
        ResponseText: Text;
        JsonArray: JsonArray;
        EndpointUrl: Text;
    begin
        // Use Admin API to get all refreshable datasets for all capacities (tenant-wide)
        EndpointUrl := PowerBIHttpClient.BuildApiUrl('admin/capacities/refreshables?$top=1000');

        // Execute request
        if not PowerBIHttpClient.ExecuteGetRequest(EndpointUrl, ResponseText) then begin
            Error('Failed to execute GET request for datasets.');
            exit(false);
        end;

        // Validate and parse response
        if not PowerBIHttpClient.ValidateJsonArrayResponse(ResponseText, JsonArray) then begin
            Error('Failed to parse JSON array response for datasets.');
            exit(false);
        end;

        // Process all refreshable datasets, filter by WorkspaceId
        ProcessRefreshableDatasets(JsonArray, WorkspaceId);
        exit(true);
    end;

    /// <summary>
    /// Processes refreshable datasets from /admin/capacities/refreshables
    /// </summary>
    /// <param name="JsonArray">Array of refreshable objects from API</param>
    /// <param name="WorkspaceId">The workspace to filter for</param>
    local procedure ProcessRefreshableDatasets(JsonArray: JsonArray; WorkspaceId: Guid)
    var
        JToken: JsonToken;
        JObject: JsonObject;
        DatasetWorkspaceId: Guid;
        PowerBIJsonProcessor: Codeunit "Power BI Json Processor";
    begin
        foreach JToken in JsonArray do begin
            JObject := JToken.AsObject();
            DatasetWorkspaceId := PowerBIJsonProcessor.GetGuidValue(JObject, 'workspaceId');
            if DatasetWorkspaceId = WorkspaceId then
                UpsertRefreshableDataset(JObject, WorkspaceId);
        end;
    end;

    /// <summary>
    /// Upserts a refreshable dataset object in BC
    /// </summary>
    /// <param name="JObject">The dataset object</param>
    /// <param name="WorkspaceId">The workspace ID</param>
    local procedure UpsertRefreshableDataset(JObject: JsonObject; WorkspaceId: Guid)
    var
        DatasetRec: Record "Power BI Dataset";
        PowerBIJsonProcessor: Codeunit "Power BI Json Processor";
        DatasetId: Guid;
        Name: Text;
        IsNew: Boolean;
        EmptyGuid: Guid;
    begin
        DatasetId := PowerBIJsonProcessor.GetGuidValue(JObject, 'objectId');
        Name := PowerBIJsonProcessor.GetTextValue(JObject, 'name', '');

        // Debug output for dataset ID
        Message('Processing DatasetId: %1, Name: %2', DatasetId, Name);

        // Skip if DatasetId is empty or all-zero GUID
        EmptyGuid := '00000000-0000-0000-0000-000000000000';
        if (DatasetId = EmptyGuid) or (DatasetId = CreateGuid()) or (Name = '') then
            exit;

        DatasetRec.SetRange("Workspace ID", WorkspaceId);
        DatasetRec.SetRange("Dataset ID", DatasetId);
        IsNew := not DatasetRec.FindFirst();
        if IsNew then
            DatasetRec.Init();
        DatasetRec."Workspace ID" := WorkspaceId;
        DatasetRec."Dataset ID" := DatasetId;
        DatasetRec."Dataset Name" := Name;
        // ... set other fields as needed ...
        if IsNew then
            DatasetRec.Insert(true)
        else
            DatasetRec.Modify(true);
    end;

    /// <summary>
    /// Triggers a refresh for a specific dataset
    /// </summary>
    /// <param name="WorkspaceId">The workspace ID</param>
    /// <param name="DatasetId">The dataset ID</param>
    /// <returns>True if refresh was successfully triggered</returns>
    procedure TriggerDatasetRefresh(WorkspaceId: Guid; DatasetId: Guid): Boolean
    var
        ResponseText: Text;
        EndpointUrl: Text;
        RequestBody: Text;
    begin
        // Build API URL (Admin endpoint for service principal)
        EndpointUrl := PowerBIHttpClient.BuildApiUrl('admin/datasets/' + PowerBIHttpClient.FormatGuidForUrl(DatasetId) + '/refreshes');

        // Empty JSON body for refresh trigger
        RequestBody := '{}';

        // Execute POST request
        if not PowerBIHttpClient.ExecutePostRequest(EndpointUrl, RequestBody, ResponseText) then begin
            Error('Failed to trigger dataset refresh for DatasetId %1.', DatasetId);
            exit(false);
        end;
        exit(true);
    end;

    /// <summary>
    /// Gets refresh history for a specific dataset
    /// </summary>
    /// <param name="WorkspaceId">The workspace ID</param>
    /// <param name="DatasetId">The dataset ID</param>
    /// <returns>True if refresh history was successfully retrieved</returns>
    procedure GetDatasetRefreshHistory(WorkspaceId: Guid; DatasetId: Guid): Boolean
    var
        ResponseText: Text;
        JsonArray: JsonArray;
        EndpointUrl: Text;
    begin
        // Build API URL with top 5 results (Admin endpoint for service principal)
        EndpointUrl := PowerBIHttpClient.BuildApiUrl('admin/datasets/' + PowerBIHttpClient.FormatGuidForUrl(DatasetId) + '/refreshes?$top=5');

        // Execute request
        if not PowerBIHttpClient.ExecuteGetRequest(EndpointUrl, ResponseText) then begin
            Error('Failed to get dataset refresh history for DatasetId %1.', DatasetId);
            exit(false);
        end;

        // Validate and parse response
        if not PowerBIHttpClient.ValidateJsonArrayResponse(ResponseText, JsonArray) then begin
            Error('Failed to parse refresh history JSON for DatasetId %1.', DatasetId);
            exit(false);
        end;

        // Process refresh history
        ProcessRefreshHistory(JsonArray, WorkspaceId, DatasetId);
        exit(true);
    end;

    // Removed legacy ProcessDatasets and CreateOrUpdateDataset procedures (no longer used with Admin API)

    local procedure ProcessRefreshHistory(JsonArray: JsonArray; WorkspaceId: Guid; DatasetId: Guid)
    var
        PowerBIDataset: Record "Power BI Dataset";
        PowerBIWorkspace: Record "Power BI Workspace";

        JToken: JsonToken;
        JObject: JsonObject;
        RefreshType: Text;
        Status: Text;
        StartTime: DateTime;
        EndTime: DateTime;
        DurationMinutes: Decimal;
        TotalDuration: Decimal;
        RefreshCount: Integer;
        DatasetName: Text;
        WorkspaceName: Text;
    begin
        if not PowerBIDataset.Get(DatasetId, WorkspaceId) then
            exit;

        DatasetName := PowerBIDataset."Dataset Name";

        // Get workspace name for history records
        if PowerBIWorkspace.Get(WorkspaceId) then
            WorkspaceName := PowerBIWorkspace."Workspace Name";

        TotalDuration := 0;
        RefreshCount := 0;

        foreach JToken in JsonArray do begin
            JObject := JToken.AsObject();

            // Extract refresh information using helper
            if PowerBIJsonProcessor.ExtractRefreshInfo(JObject, RefreshType, Status, StartTime, EndTime) then begin
                RefreshCount += 1;

                // Store detailed refresh history
                StoreDatasetRefreshHistoryRecord(JObject, DatasetId, DatasetName, WorkspaceId, WorkspaceName);

                // Update latest refresh info on first iteration (most recent)
                if RefreshCount = 1 then
                    UpdateLatestRefreshInfo(PowerBIDataset, Status, StartTime, EndTime);

                // Calculate duration and add to total
                if EndTime <> 0DT then begin
                    DurationMinutes := PowerBIJsonProcessor.CalculateDurationMinutes(StartTime, EndTime);
                    TotalDuration += DurationMinutes;
                end;
            end;
        end;

        // Calculate and update averages
        if RefreshCount > 0 then
            UpdateRefreshStatistics(PowerBIDataset, TotalDuration, RefreshCount);

        PowerBIDataset.Modify();
    end;

    local procedure StoreDatasetRefreshHistoryRecord(JObject: JsonObject; DatasetId: Guid; DatasetName: Text; WorkspaceId: Guid; WorkspaceName: Text)
    var
        RefreshHistory: Record "PBI Dataset Refresh History";
        RefreshId: Text;
        StartTime: DateTime;
        EndTime: DateTime;
        Status: Text;
        RefreshType: Text;
        ErrorMessage: Text;
        ServiceExceptionJson: Text;
    begin
        // Extract refresh ID
        RefreshId := PowerBIJsonProcessor.GetTextValue(JObject, 'requestId', '');
        if RefreshId = '' then
            RefreshId := PowerBIJsonProcessor.GetTextValue(JObject, 'id', '');

        if RefreshId = '' then
            exit;

        // Check if record already exists
        RefreshHistory.SetRange("Refresh ID", RefreshId);
        if RefreshHistory.FindFirst() then
            exit; // Already processed

        // Create new record
        RefreshHistory.Init();
        RefreshHistory."Dataset ID" := Format(DatasetId);
        RefreshHistory."Dataset Name" := CopyStr(DatasetName, 1, MaxStrLen(RefreshHistory."Dataset Name"));
        RefreshHistory."Workspace ID" := Format(WorkspaceId);
        RefreshHistory."Workspace Name" := CopyStr(WorkspaceName, 1, MaxStrLen(RefreshHistory."Workspace Name"));
        RefreshHistory."Refresh ID" := CopyStr(RefreshId, 1, MaxStrLen(RefreshHistory."Refresh ID"));

        // Extract and store refresh details
        StartTime := PowerBIJsonProcessor.GetDateTimeValue(JObject, 'startTime');
        RefreshHistory."Start Time" := StartTime;

        EndTime := PowerBIJsonProcessor.GetDateTimeValue(JObject, 'endTime');
        RefreshHistory."End Time" := EndTime;

        RefreshHistory.CalculateDuration();

        Status := PowerBIJsonProcessor.GetTextValue(JObject, 'status', '');
        RefreshHistory."Status" := ConvertToRefreshStatus(Status);

        RefreshType := PowerBIJsonProcessor.GetTextValue(JObject, 'refreshType', '');
        RefreshHistory."Refresh Type" := CopyStr(RefreshType, 1, MaxStrLen(RefreshHistory."Refresh Type"));

        // Handle error information
        if RefreshHistory."Status" = RefreshHistory."Status"::Failed then begin
            ErrorMessage := PowerBIJsonProcessor.GetTextValue(JObject, 'serviceExceptionJson', '');
            if ErrorMessage = '' then
                ErrorMessage := PowerBIJsonProcessor.GetTextValue(JObject, 'error', '');

            RefreshHistory."Error Message" := CopyStr(ErrorMessage, 1, MaxStrLen(RefreshHistory."Error Message"));

            ServiceExceptionJson := PowerBIJsonProcessor.GetTextValue(JObject, 'serviceExceptionJson', '');
            if ServiceExceptionJson <> '' then
                RefreshHistory.SetServiceExceptionText(ServiceExceptionJson);
        end;

        RefreshHistory.Insert(true);
    end;

    local procedure ConvertToRefreshStatus(StatusText: Text): Enum "Power BI Refresh Status"
    var
        RefreshStatus: Enum "Power BI Refresh Status";
    begin
        case LowerCase(StatusText) of
            'completed':
                exit(RefreshStatus::Completed);
            'failed':
                exit(RefreshStatus::Failed);
            'inprogress', 'in progress':
                exit(RefreshStatus::"In Progress");
            'disabled':
                exit(RefreshStatus::Disabled);
            'notstarted', 'not started':
                exit(RefreshStatus::NotStarted);
            else
                exit(RefreshStatus::Unknown);
        end;
    end;

    local procedure UpdateLatestRefreshInfo(var PowerBIDataset: Record "Power BI Dataset"; Status: Text; StartTime: DateTime; EndTime: DateTime)
    var
        DurationMinutes: Decimal;
    begin
        PowerBIDataset."Last Refresh" := StartTime;
        PowerBIDataset."Last Refresh Status" := CopyStr(Status, 1, MaxStrLen(PowerBIDataset."Last Refresh Status"));

        if EndTime <> 0DT then begin
            DurationMinutes := PowerBIJsonProcessor.CalculateDurationMinutes(StartTime, EndTime);
            PowerBIDataset."Last Refresh Duration (Min)" := DurationMinutes;
        end;
    end;

    local procedure UpdateRefreshStatistics(var PowerBIDataset: Record "Power BI Dataset"; TotalDuration: Decimal; RefreshCount: Integer)
    begin
        if RefreshCount > 0 then begin
            PowerBIDataset."Average Refresh Duration (Min)" := TotalDuration / RefreshCount;
            PowerBIDataset."Refresh Count" := RefreshCount;
        end;
    end;

    /// <summary>
    /// Gets all refreshable datasets in a workspace
    /// </summary>
    /// <param name="WorkspaceId">The workspace ID</param>
    /// <param name="RefreshableDatasets">Record set of refreshable datasets</param>
    /// <returns>Number of refreshable datasets found</returns>
    procedure GetRefreshableDatasets(WorkspaceId: Guid; var RefreshableDatasets: Record "Power BI Dataset"): Integer
    begin
        RefreshableDatasets.Reset();
        RefreshableDatasets.SetRange("Workspace ID", WorkspaceId);
        RefreshableDatasets.SetRange("Is Refreshable", true);
        exit(RefreshableDatasets.Count());
    end;

    /// <summary>
    /// Gets datasets with failed last refresh
    /// </summary>
    /// <param name="FailedDatasets">Record set of datasets with failed refresh</param>
    /// <returns>Number of datasets with failed refresh</returns>
    procedure GetFailedRefreshDatasets(var FailedDatasets: Record "Power BI Dataset"): Integer
    begin
        FailedDatasets.Reset();
        FailedDatasets.SetFilter("Last Refresh Status", '*Failed*|*Error*');
        exit(FailedDatasets.Count());
    end;

    /// <summary>
    /// Calculates average refresh time across all datasets
    /// </summary>
    /// <returns>Average refresh time in minutes</returns>
    procedure CalculateAverageRefreshTime(): Decimal
    var
        PowerBIDataset: Record "Power BI Dataset";
        TotalTime: Decimal;
        DatasetCount: Integer;
    begin
        PowerBIDataset.Reset();
        PowerBIDataset.SetFilter("Average Refresh Duration (Min)", '>0');

        if PowerBIDataset.FindSet() then
            repeat
                TotalTime += PowerBIDataset."Average Refresh Duration (Min)";
                DatasetCount += 1;
            until PowerBIDataset.Next() = 0;

        if DatasetCount > 0 then
            exit(TotalTime / DatasetCount);

        exit(0);
    end;
}