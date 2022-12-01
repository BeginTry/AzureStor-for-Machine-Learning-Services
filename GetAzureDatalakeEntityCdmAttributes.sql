CREATE OR ALTER PROCEDURE dbo.GetAzureDatalakeEntityCdmAttributes
/******************************************************************************
* Name     : dbo.GetAzureDatalakeEntityCdmAttributes
* Purpose  : Gets the Common Data Model entity attributes from a *.cdm.json 
*				file in Azure Data Lake. This can be used to design a table
*				to store data from corresponding CSV files.
* Inputs   : 
*	@AzureEndpoint - example: 'https://yourstorageaccountname.dfs.core.windows.net/'
*	@EndpointKey - an Access Key for the storage account (from the Azure portal).
*	@ContainerName - a container name for the storage account.
*	@JsonFile - the "path" to the Common Data Model (CDM) JSON file for a specific entity, 
*			starting with the root folder in the container.
* Outputs  : 
* Returns  : 
******************************************************************************
* Change History
*	2022-11-29	DMason	Created.
******************************************************************************/
	@AzureEndpoint NVARCHAR(2048),
	@EndpointKey NVARCHAR(256),
	@ContainerName NVARCHAR(256),
	@JsonFile NVARCHAR(2048)
AS
BEGIN
	DECLARE @RScript NVARCHAR(MAX) = '
	#install.packages("jsonlite", dependencies=TRUE, repos="http://cran.rstudio.com/")
	library(AzureStor)
	library("jsonlite")

	AzureEndpoint <- "' + @AzureEndpoint + '"
	EndpointKey <- "' + @EndpointKey + '"
	ContainerName <- "' + @ContainerName + '"
	jsonFile <- "' + @JsonFile + '"

	dataLakeEndpoint <- AzureStor::storage_endpoint(endpoint = AzureEndpoint, key = EndpointKey)
	cont <- AzureStor::storage_container(endpoint = dataLakeEndpoint, name = ContainerName)

	rawvec <- storage_download(cont, src=jsonFile, dest=NULL)
	jsonChar <- rawToChar(rawvec)
	df <- as.data.frame(jsonlite::fromJSON(jsonChar))
	EntityCdmAttributes <- as.data.frame(df$definitions.hasAttributes)
'

	EXEC sp_execute_external_script   
		@language = N'R',
		@script = @RScript,
		@output_data_1_name = N'EntityCdmAttributes';
END
GO
