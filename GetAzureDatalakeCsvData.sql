CREATE OR ALTER PROCEDURE dbo.GetAzureDatalakeCsvData
/******************************************************************************
* Name     : dbo.GetAzureDatalakeCsvData
* Purpose  : Returns data from an Azure Data Lake CSV file as a tabular result set.
* Inputs   : 
*	@AzureEndpoint - example: 'https://yourstorageaccountname.dfs.core.windows.net/'
*	@EndpointKey - an Access Key for the storage account (from the Azure portal).
*	@ContainerName - a container name for the storage account.
*	@csvFile - the "path" to the CSV file, starting with the root folder in the container.
* Outputs  : 
* Returns  : 
******************************************************************************
* Change History
*	2022-11-29	DMason	Created.
******************************************************************************/
	@AzureEndpoint NVARCHAR(2048),
	@EndpointKey NVARCHAR(256),
	@ContainerName NVARCHAR(256),
	@CsvFile NVARCHAR(2048)
AS
BEGIN
	DECLARE @RScript NVARCHAR(MAX) = '
	library(AzureStor)

	AzureEndpoint <- "' + @AzureEndpoint + '"
	EndpointKey <- "' + @EndpointKey + '"
	ContainerName <- "' + @ContainerName + '"
	csvFile <- "' + @csvFile + '"

	dataLakeEndpoint <- AzureStor::storage_endpoint(endpoint = AzureEndpoint, key = EndpointKey)
	cont <- AzureStor::storage_container(endpoint = dataLakeEndpoint, name = ContainerName)

	dfCsvData <- as.data.frame(
	  AzureStor::storage_read_csv(container = cont, file = csvFile, col_names = FALSE)
	)
	';

	EXEC sp_execute_external_script   
		@language = N'R',
		@script = @RScript,
		@output_data_1_name = N'dfCsvData';
END
GO
