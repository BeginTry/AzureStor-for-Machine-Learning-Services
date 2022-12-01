CREATE OR ALTER PROCEDURE dbo.BulkLoadYourTable
/******************************************************************************
* Name     : dbo.BulkLoadYourTable
* Purpose  : 
* Inputs   : 
*	@AzureEndpoint - example: 'https://yourstorageaccountname.dfs.core.windows.net/'
*	@EndpointKey - an Access Key for the storage account (from the Azure portal).
*	@ContainerName - a container name for the storage account.
*	@DirectoryName - the "path" to the directory, starting with the root folder in the container.
* Outputs  : 
* Returns  : 
******************************************************************************
* Change History
*	2022-11-29	DMason	Created.
******************************************************************************/
	@AzureEndpoint NVARCHAR(2048),
	@EndpointKey NVARCHAR(256),
	@ContainerName NVARCHAR(256),
	@DirectoryName NVARCHAR(2048)
AS
BEGIN
	CREATE TABLE #Files (
		"Name" NVARCHAR(256),
		"Size" BIGINT,
		"IsDir" SMALLINT,
		"LastModified" DATETIME2,
		"Permissions" NVARCHAR(64),
		"Etag" NVARCHAR(128)
	)
	WITH(DATA_COMPRESSION = ROW);

	INSERT INTO #Files
	EXEC dbo.GetAzureDatalakeFileListFromFolder
		@AzureEndpoint = @AzureEndpoint,
		@EndpointKey = @EndpointKey,
		@ContainerName = @ContainerName,
		@DirectoryName = @DirectoryName,
		@FileExtensionFilter = N'csv'

	DROP TABLE IF EXISTS #Errors;
	CREATE TABLE #Errors (
		[Filename] NVARCHAR(256),
		ErrorMessage NVARCHAR(4000),
		ErrorSeverity INT,
		ErrorState INT,
	)
	WITH(DATA_COMPRESSION = ROW);

	DECLARE @Filename NVARCHAR(256);
	DECLARE curFiles CURSOR FOR
		SELECT [Name] FROM #Files;
	OPEN curFiles;
	FETCH NEXT FROM curFiles INTO @FileName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT @Filename;

		BEGIN TRAN;
		BEGIN TRY
			INSERT INTO dbo.YourTable
			EXEC dbo.GetAzureDatalakeCsvData
				@AzureEndpoint = @AzureEndpoint,
				@EndpointKey = @EndpointKey,
				@ContainerName = @ContainerName,
				@CsvFile = @Filename
		END TRY
		BEGIN CATCH
			INSERT INTO #Errors([Filename], ErrorMessage, ErrorSeverity, ErrorState)
			VALUES(@Filename, ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
		END CATCH
		COMMIT;

		FETCH NEXT FROM curFiles INTO @FileName;
	END

	CLOSE curFiles;
	DEALLOCATE curFiles;

	IF EXISTS (SELECT * FROM #Errors)
	BEGIN
		DECLARE @Msg NVARCHAR(2047);
		SELECT @Msg = CONCAT_WS(' ', 'There were', COUNT(*), 'bulk load errors for table [dbo].[YourTable]') 
		FROM #Errors;

		RAISERROR(@Msg, 16, 1);
		SELECT * FROM #Errors;
	END

END
GO
