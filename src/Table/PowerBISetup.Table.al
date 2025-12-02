table 90112 "Power BI Setup"
{
    Caption = 'Power BI Setup';
    DataClassification = CustomerContent;

    fields
    {
        field(1; "Primary Key"; Code[10])
        {
            Caption = 'Primary Key';
        }

        field(2; "Client ID"; Text[100])
        {
            Caption = 'Client ID';
            ToolTip = 'Specifies the Azure AD Application (Client) ID for Power BI API access.';
        }

        field(3; "Client Secret"; Text[250])
        {
            Caption = 'Client Secret';
            ExtendedDatatype = Masked;
            ToolTip = 'Specifies the Azure AD Application Client Secret for Power BI API access.';
        }

        field(4; "Tenant ID"; Text[100])
        {
            Caption = 'Tenant ID';
            ToolTip = 'Specifies the Azure AD Tenant ID for authentication.';
        }

        field(5; "Authority URL"; Text[250])
        {
            Caption = 'Authority URL';
            ToolTip = 'Specifies the Azure AD authority URL for authentication.';
        }

        field(6; "Power BI API URL"; Text[250])
        {
            Caption = 'Power BI API URL';
            ToolTip = 'Specifies the Power BI REST API base URL.';
        }

        field(7; "Auto Sync Enabled"; Boolean)
        {
            Caption = 'Auto Sync Enabled';
            ToolTip = 'Specifies whether automatic synchronization is enabled.';

            trigger OnValidate()
            var
                PowerBIAutoSync: Codeunit "Power BI Auto Sync";
                JobQueueEntry: Record "Job Queue Entry";
            begin
                if "Auto Sync Enabled" then begin
                    // Validate sync frequency is set
                    if "Sync Frequency (Hours)" = 0 then
                        Error('Please set Sync Frequency (Hours) before enabling Auto Sync.');

                    // Check if job queue entry exists, if not create it
                    if IsNullGuid("Job Queue Entry ID") or not JobQueueEntry.Get("Job Queue Entry ID") then begin
                        // Clear invalid reference
                        Clear("Job Queue Entry ID");

                        // Create new job queue entry
                        if PowerBIAutoSync.CreateJobQueueEntry() then
                            Message('Auto Sync has been enabled. A new job queue entry was created.')
                        else
                            Error('Failed to create Job Queue Entry. Please check Job Queue settings.');
                    end else begin
                        // Entry exists, just enable it
                        PowerBIAutoSync.SetJobQueueEntryStatus(true);
                        Message('Auto Sync has been enabled.');
                    end;
                end else begin
                    // Disable auto sync
                    if not IsNullGuid("Job Queue Entry ID") and JobQueueEntry.Get("Job Queue Entry ID") then
                        PowerBIAutoSync.SetJobQueueEntryStatus(false);

                    Message('Auto Sync has been disabled.');
                end;
            end;
        }

        field(8; "Sync Frequency (Hours)"; Integer)
        {
            Caption = 'Sync Frequency (Hours)';
            ToolTip = 'Specifies how often to automatically sync data from Power BI (in hours).';
            MinValue = 1;
            MaxValue = 168; // 7 days

            trigger OnValidate()
            var
                PowerBIAutoSync: Codeunit "Power BI Auto Sync";
                JobQueueEntry: Record "Job Queue Entry";
            begin
                if "Sync Frequency (Hours)" < 1 then
                    Error('Sync Frequency must be at least 1 hour.');

                if "Sync Frequency (Hours)" > 168 then
                    Error('Sync Frequency cannot exceed 168 hours (7 days).');

                // If auto sync is enabled and job queue exists, update it
                if "Auto Sync Enabled" then begin
                    if not IsNullGuid("Job Queue Entry ID") and JobQueueEntry.Get("Job Queue Entry ID") then begin
                        PowerBIAutoSync.UpdateJobQueueFrequency("Sync Frequency (Hours)");
                        Message('Sync frequency updated to %1 hour(s).', "Sync Frequency (Hours)");
                    end else begin
                        // Job queue entry is missing, offer to recreate
                        if Confirm('Job Queue Entry is missing. Would you like to recreate it?', true) then begin
                            Clear("Job Queue Entry ID");
                            "Auto Sync Enabled" := false;
                            Modify();
                            Commit();

                            // Re-enable to create new entry
                            "Auto Sync Enabled" := true;
                            Modify();
                        end else begin
                            "Auto Sync Enabled" := false;
                        end;
                    end;
                end;
            end;
        }

        field(9; "Last Auto Sync"; DateTime)
        {
            Caption = 'Last Auto Sync';
            ToolTip = 'Specifies when the last automatic sync was performed.';
        }

        field(10; "Job Queue Entry ID"; Guid)
        {
            Caption = 'Job Queue Entry ID';
            DataClassification = SystemMetadata;
            Editable = false;
            ToolTip = 'Specifies the ID of the Job Queue Entry used for automatic synchronization.';

            trigger OnValidate()
            var
                JobQueueEntry: Record "Job Queue Entry";
            begin
                // Validate that the job queue entry actually exists
                if not IsNullGuid("Job Queue Entry ID") then begin
                    if not JobQueueEntry.Get("Job Queue Entry ID") then begin
                        // Entry doesn't exist, clear the reference
                        Clear("Job Queue Entry ID");
                        "Auto Sync Enabled" := false;
                        Message('The referenced Job Queue Entry no longer exists. ' +
                                'Auto Sync has been disabled. Please re-enable it to create a new entry.');
                    end;
                end;
            end;
        }

        field(11; "Last Sync Duration (Sec)"; Integer)
        {
            Caption = 'Last Sync Duration (Sec)';
            DataClassification = SystemMetadata;
            Editable = false;
            ToolTip = 'Specifies the duration of the last sync operation in seconds.';
        }
    }

    keys
    {
        key(PK; "Primary Key")
        {
            Clustered = true;
        }
    }

    trigger OnInsert()
    begin
        SetDefaults();
    end;

    trigger OnDelete()
    var
        PowerBIAutoSync: Codeunit "Power BI Auto Sync";
    begin
        // Clean up job queue entry when setup is deleted
        if "Auto Sync Enabled" then
            PowerBIAutoSync.DeleteJobQueueEntry();
    end;

    local procedure SetDefaults()
    begin
        "Primary Key" := '';
        "Authority URL" := 'https://login.microsoftonline.com/';
        "Power BI API URL" := 'https://api.powerbi.com/v1.0/myorg/';
        "Sync Frequency (Hours)" := 24;
    end;

    /// <summary>
    /// Validates that the Job Queue Entry reference is valid
    /// </summary>
    /// <returns>True if the Job Queue Entry exists and is valid</returns>
    procedure ValidateJobQueueEntry(): Boolean
    var
        JobQueueEntry: Record "Job Queue Entry";
    begin
        if IsNullGuid("Job Queue Entry ID") then
            exit(false);

        if not JobQueueEntry.Get("Job Queue Entry ID") then begin
            // Entry doesn't exist anymore, clean up
            Clear("Job Queue Entry ID");
            "Auto Sync Enabled" := false;
            Modify();
            exit(false);
        end;

        exit(true);
    end;

    /// <summary>
    /// Recreates the Job Queue Entry (useful when fixing orphaned references)
    /// </summary>
    /// <returns>True if recreation was successful</returns>
    procedure RecreateJobQueueEntry(): Boolean
    var
        PowerBIAutoSync: Codeunit "Power BI Auto Sync";
    begin
        // Delete old entry if it exists
        if "Auto Sync Enabled" then
            PowerBIAutoSync.DeleteJobQueueEntry();

        // Clear reference
        Clear("Job Queue Entry ID");
        "Auto Sync Enabled" := false;
        Modify();
        Commit();

        // Create new entry
        if "Sync Frequency (Hours)" = 0 then
            "Sync Frequency (Hours)" := 24;

        "Auto Sync Enabled" := true;
        Modify();

        exit(ValidateJobQueueEntry());
    end;
}