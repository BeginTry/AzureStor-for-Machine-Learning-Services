CREATE OR ALTER PROCEDURE dbo.GetAzureDatalakeFileListFromFolder
/******************************************************************************
* Name     : dbo.GetAzureDatalakeFileListFromFolder
* Purpose  : Returns a list of files in an Azure Data Lake folder.
* Inputs   : 
*	@AzureEndpoint - example: 'https://yourstorageaccountname.dfs.core.windows.net/'
*	@EndpointKey - an Access Key for the storage account (from the Azure portal).
*	@ContainerName - a container name for the storage account.
*	@DirectoryName - the "path" to the directory, starting with the root folder in the container.
*	@FileExtensionFilter - optional filter for specific file types.
*		Examples: 'csv', 'txt', etc.
* Outputs  : 
* Returns  : 
******************************************************************************
* Change History
*	2022-11-29	DMason	Created.
******************************************************************************/
	@AzureEndpoint NVARCHAR(2048),
	@EndpointKey NVARCHAR(256),
	@ContainerName NVARCHAR(256),
	@DirectoryName NVARCHAR(2048),
	@FileExtensionFilter NVARCHAR(16) = NULL
AS
BEGIN
	DECLARE @RScript NVARCHAR(MAX) = '
	library(AzureStor)

	AzureEndpoint <- "' + @AzureEndpoint + '"
	EndpointKey <- "' + @EndpointKey + '"
	ContainerName <- "' + @ContainerName + '"
	DirectoryName <- "' + @DirectoryName + '"

	dataLakeEndpoint <- AzureStor::storage_endpoint(endpoint = AzureEndpoint, key = EndpointKey)
	cont <- AzureStor::storage_container(endpoint = dataLakeEndpoint, name = ContainerName)
	dfFiles <- AzureStor::list_storage_files(container = cont, dir = DirectoryName)' + 
		CASE
			WHEN @FileExtensionFilter IS NULL THEN ''
			ELSE '
	dfFiles <- dfFiles[endsWith(dfFiles$name, "' + @FileExtensionFilter + '"),]'
		END;

	EXEC sp_execute_external_script   
		@language = N'R',
		@script = @RScript,
		@output_data_1_name = N'dfFiles';
END
GO
