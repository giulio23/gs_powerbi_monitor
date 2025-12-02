codeunit 90111 "Power BI Auto Sync"
{
    // This codeunit handles automatic synchronization of Power BI data
    // It's designed to be called by job queue entries

    trigger OnRun()
    begin
        RunAutoSync();
    end;

    procedure RunAutoSync()
    var
        PowerBISetup: Record "Power BI Setup";
        PowerBIAPIOrchestrator: Codeunit "Power BI API Orchestrator";
        SyncSuccessful: Boolean;
    begin
        // Get setup record
        if not PowerBISetup.Get('') then
            exit;

        // Check if auto sync is enabled
        if not PowerBISetup."Auto Sync Enabled" then
            exit;

        // Check if sync is due
        if not IsSyncDue(PowerBISetup) then
            exit;

        // Run synchronization
        SyncSuccessful := false;
        if PowerBIAPIOrchestrator.SynchronizeAllData() then begin
            SyncSuccessful := true;

            // Update last sync time
            PowerBISetup."Last Auto Sync" := CurrentDateTime();
            PowerBISetup.Modify();
        end;

        // Log the sync attempt
        LogSyncAttempt(SyncSuccessful);
    end;

    procedure IsSyncDue(PowerBISetup: Record "Power BI Setup"): Boolean
    var
        HoursSinceLastSync: Decimal;
        SyncFrequencyHours: Integer;
    begin
        // If never synced before, sync now
        if PowerBISetup."Last Auto Sync" = 0DT then
            exit(true);

        // Calculate hours since last sync
        HoursSinceLastSync := (CurrentDateTime() - PowerBISetup."Last Auto Sync") / (1000 * 60 * 60); // Convert milliseconds to hours

        // Get sync frequency (default to 24 hours if not set)
        SyncFrequencyHours := PowerBISetup."Sync Frequency (Hours)";
        if SyncFrequencyHours <= 0 then
            SyncFrequencyHours := 24;

        // Check if it's time to sync
        exit(HoursSinceLastSync >= SyncFrequencyHours);
    end;

    procedure CreateJobQueueEntry(): Boolean
    var
        JobQueueEntry: Record "Job Queue Entry";
        PowerBISetup: Record "Power BI Setup";
    begin
        // Get setup to determine frequency
        if not PowerBISetup.Get('') then
            exit(false);

        // Check if entry already exists
        if not IsNullGuid(PowerBISetup."Job Queue Entry ID") then begin
            if JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
                // Entry already exists, just update it
                UpdateExistingJobQueueEntry(JobQueueEntry, PowerBISetup);
                exit(true);
            end;
        end;

        // Delete any existing entries (cleanup)
        DeleteJobQueueEntry();

        // Create new job queue entry
        JobQueueEntry.Init();
        JobQueueEntry."Object Type to Run" := JobQueueEntry."Object Type to Run"::Codeunit;
        JobQueueEntry."Object ID to Run" := Codeunit::"Power BI Auto Sync";
        JobQueueEntry."Job Queue Category Code" := 'POWERBI';
        JobQueueEntry.Description := 'Power BI Auto Synchronization';
        JobQueueEntry."User ID" := CopyStr(UserId(), 1, MaxStrLen(JobQueueEntry."User ID"));

        // Set to run every hour (we'll check internally if sync is actually due)
        JobQueueEntry."Recurring Job" := true;
        JobQueueEntry."Run on Mondays" := true;
        JobQueueEntry."Run on Tuesdays" := true;
        JobQueueEntry."Run on Wednesdays" := true;
        JobQueueEntry."Run on Thursdays" := true;
        JobQueueEntry."Run on Fridays" := true;
        JobQueueEntry."Run on Saturdays" := true;
        JobQueueEntry."Run on Sundays" := true;
        JobQueueEntry."Starting Time" := Time();
        JobQueueEntry."No. of Minutes between Runs" := 60; // Check every hour

        // Insert and set to ready
        if not JobQueueEntry.Insert(true) then
            exit(false);

        // Store the Job Queue Entry ID in setup
        PowerBISetup."Job Queue Entry ID" := JobQueueEntry.ID;
        PowerBISetup.Modify();
        Commit(); // Ensure the reference is saved

        // Set to ready status
        JobQueueEntry.SetStatus(JobQueueEntry.Status::Ready);
        exit(true);
    end;

    procedure DeleteJobQueueEntry()
    var
        JobQueueEntry: Record "Job Queue Entry";
        PowerBISetup: Record "Power BI Setup";
    begin
        // Get setup record
        if PowerBISetup.Get('') then begin
            // Delete by GUID if we have it
            if not IsNullGuid(PowerBISetup."Job Queue Entry ID") then begin
                if JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
                    JobQueueEntry.Cancel();
                    JobQueueEntry.Delete();
                end;
                
                // Clear the reference
                Clear(PowerBISetup."Job Queue Entry ID");
                PowerBISetup."Auto Sync Enabled" := false;
                PowerBISetup.Modify();
            end;
        end;

        // Also clean up any orphaned entries (by codeunit ID)
        JobQueueEntry.SetRange("Object Type to Run", JobQueueEntry."Object Type to Run"::Codeunit);
        JobQueueEntry.SetRange("Object ID to Run", Codeunit::"Power BI Auto Sync");
        if JobQueueEntry.FindSet() then
            repeat
                JobQueueEntry.Cancel();
                JobQueueEntry.Delete();
            until JobQueueEntry.Next() = 0;
    end;

    procedure IsJobQueueEntryActive(): Boolean
    var
        JobQueueEntry: Record "Job Queue Entry";
    begin
        JobQueueEntry.SetRange("Object Type to Run", JobQueueEntry."Object Type to Run"::Codeunit);
        JobQueueEntry.SetRange("Object ID to Run", Codeunit::"Power BI Auto Sync");
        JobQueueEntry.SetFilter(Status, '%1|%2', JobQueueEntry.Status::Ready, JobQueueEntry.Status::"In Process");
        exit(not JobQueueEntry.IsEmpty());
    end;

    /// <summary>
    /// Sets the status of the Job Queue Entry (Enable/Disable)
    /// </summary>
    /// <param name="Enable">True to enable (Ready), False to disable (On Hold)</param>
    procedure SetJobQueueEntryStatus(Enable: Boolean)
    var
        JobQueueEntry: Record "Job Queue Entry";
        PowerBISetup: Record "Power BI Setup";
    begin
        if not PowerBISetup.Get('') then
            exit;

        // If enabling and entry doesn't exist, create it
        if Enable then begin
            if IsNullGuid(PowerBISetup."Job Queue Entry ID") or not JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
                CreateJobQueueEntry();
                exit;
            end;
        end;

        // Get the job queue entry
        if not IsNullGuid(PowerBISetup."Job Queue Entry ID") then begin
            if JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
                if Enable then
                    JobQueueEntry.SetStatus(JobQueueEntry.Status::Ready)
                else
                    JobQueueEntry.SetStatus(JobQueueEntry.Status::"On Hold");
            end;
        end;
    end;

    /// <summary>
    /// Updates the frequency of the Job Queue Entry
    /// </summary>
    /// <param name="FrequencyHours">The new frequency in hours</param>
    procedure UpdateJobQueueFrequency(FrequencyHours: Integer)
    var
        JobQueueEntry: Record "Job Queue Entry";
        PowerBISetup: Record "Power BI Setup";
    begin
        if not PowerBISetup.Get('') then
            exit;

        if IsNullGuid(PowerBISetup."Job Queue Entry ID") then begin
            Error('Job Queue Entry does not exist. Please disable and re-enable Auto Sync to create a new entry.');
            exit;
        end;

        if not JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
            Error('Job Queue Entry not found. Please disable and re-enable Auto Sync to create a new entry.');
            exit;
        end;

        // Update the description to reflect new frequency
        JobQueueEntry.Description := StrSubstNo('Power BI Auto Sync (Every %1 hours)', FrequencyHours);
        JobQueueEntry.Modify(true);
    end;

    /// <summary>
    /// Validates if the Job Queue Entry exists and displays its status
    /// </summary>
    procedure ValidateJobQueueEntry()
    var
        JobQueueEntry: Record "Job Queue Entry";
        PowerBISetup: Record "Power BI Setup";
        StatusText: Text;
    begin
        if not PowerBISetup.Get('') then begin
            Message('Power BI Setup record not found.');
            exit;
        end;

        if IsNullGuid(PowerBISetup."Job Queue Entry ID") then begin
            Message('No Job Queue Entry ID is stored in Power BI Setup.');
            exit;
        end;

        if not JobQueueEntry.Get(PowerBISetup."Job Queue Entry ID") then begin
            Message('Job Queue Entry with ID %1 does not exist.\' +
                    '\' +
                    'This usually happens when:\' +
                    '1. The entry was manually deleted\' +
                    '2. The extension was reinstalled\' +
                    '\' +
                    'To fix: Disable Auto Sync, then re-enable it.',
                    PowerBISetup."Job Queue Entry ID");
            exit;
        end;

        StatusText := Format(JobQueueEntry.Status);
        Message('Job Queue Entry Details:\' +
                '\' +
                'ID: %1\' +
                'Status: %2\' +
                'Description: %3\' +
                'Next Run: %4\' +
                'User ID: %5',
                JobQueueEntry.ID,
                StatusText,
                JobQueueEntry.Description,
                JobQueueEntry."Earliest Start Date/Time",
                JobQueueEntry."User ID");
    end;

    /// <summary>
    /// Forces an immediate sync regardless of schedule
    /// </summary>
    procedure ForceSync()
    var
        PowerBISetup: Record "Power BI Setup";
        PowerBIAPIOrchestrator: Codeunit "Power BI API Orchestrator";
        StartTime: DateTime;
        EndTime: DateTime;
        Duration: Integer;
    begin
        if not PowerBISetup.Get('') then
            exit;

        StartTime := CurrentDateTime();
        
        if PowerBIAPIOrchestrator.SynchronizeAllData() then begin
            EndTime := CurrentDateTime();
            Duration := (EndTime - StartTime) / 1000; // Convert to seconds
            
            PowerBISetup."Last Auto Sync" := EndTime;
            PowerBISetup."Last Sync Duration (Sec)" := Duration;
            PowerBISetup.Modify();
            
            Message('Synchronization completed successfully in %1 seconds.', Duration);
        end else
            Message('Synchronization completed with some errors. Check the error logs for details.');
    end;

    local procedure UpdateExistingJobQueueEntry(var JobQueueEntry: Record "Job Queue Entry"; PowerBISetup: Record "Power BI Setup")
    begin
        // Update description and settings if needed
        JobQueueEntry.Description := 'Power BI Auto Synchronization';
        JobQueueEntry."Recurring Job" := true;
        JobQueueEntry."Run on Mondays" := true;
        JobQueueEntry."Run on Tuesdays" := true;
        JobQueueEntry."Run on Wednesdays" := true;
        JobQueueEntry."Run on Thursdays" := true;
        JobQueueEntry."Run on Fridays" := true;
        JobQueueEntry."Run on Saturdays" := true;
        JobQueueEntry."Run on Sundays" := true;
        JobQueueEntry."No. of Minutes between Runs" := 60;
        JobQueueEntry.Modify(true);
        
        // Ensure it's set to Ready status
        if JobQueueEntry.Status <> JobQueueEntry.Status::Ready then
            JobQueueEntry.SetStatus(JobQueueEntry.Status::Ready);
    end;

    local procedure LogSyncAttempt(Success: Boolean)
    begin
        // Optional: Add logging logic here if needed
        // For now, we'll just use the Last Auto Sync field in the setup table
        // The Success parameter can be used for future logging implementation
        if Success then
            exit; // Placeholder for success logging
    end;
}