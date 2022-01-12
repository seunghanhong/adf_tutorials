# Tutorial 1

This tutorial describes how to use the Azure Data Factory to transform and load a single file into the Azure SQL Database, following a common data pipeline architecture. The pipeline that you create in this tutorial un-pivots global CO2 emission data, stores them in an Azure storage account and ingests them into the Azure SQL Database. This tutorial teaches the following steps in the Azure Data Factory.

- Create a new linked service.
- Create a new dataset.
- Create a data flow to transform the dataset.
- Create a pipeline to run the data flow.
- Run the pipeline.
- Monitor the pipeline run.
- Review results


## Prerequisites

- Data Factory
- Two storage accounts
    - The first storage account has a `data` container containing the `API_EN.ATM.CO2E.KT_DS2_en_csv_v2_3358949.csv` file.
    - The second storage account has a `conformed` container. Ensure `Hierarchical namespace` is enabled.
- Azure SQL Database (Serverless)
- (Optional) Azure DevOps repository

## Transform and Load Data From Blob To Blob

Suppose that there is a raw file (global CO2 emission), which has been extracted from a source database and landed in the `raw layer`. The picture below illustrates a typical architecture of data pipelines and the red rectangle is covered in this section, where the raw file is transformed (i.e. unpivot) and loaded to a blob storage.

![Architecture](./images/tutorial_1/architecture_data_pipeline_raw_to_conformed.png)

1. Create a new Linked service.

    a. Before transforming the raw file, a linked service should be created to read the file. Click `Manage` and then `Linked services` on the left pane. Click `New` to create a new linked service. Optionally, create a new feature branch if the Data Factory is integrated with Git.

    ![Manage > Linked service > New](./images/tutorial_1/create_new_linked_service.png)

    b. Select `Azure Blob Storage` and `Continue`.

    ![Azure Blob Storage](./images/tutorial_1/linked_service_blob_storage.png)

    c. Enter `Name` and select the `storage account name`, where the raw file is stored, to configure the linked service details as below.

    ![New linked service](./images/tutorial_1/create_linked_service_blob_storage_1.png)

    d. Click `Test connection` to check if the linked service is set up properly, and click `Create`.
    
2. Create a new Dataset.

    a. Click `Author` on the left menu, click `+ (plus)` button and then select `Dataset`.

    ![Author > Add > Dataset](./images/tutorial_1/source_dataset.png)

    b. Select `Azure Blob Storage` and click `Continue`.

    ![Azure Blob Storage](./images/tutorial_1/create_dataset_blob_storage.png)

    c. Select `DelimitedText` and click `Continue`.

    ![DelimitedText](./images/tutorial_1/create_dataset_delimited_text.png)

    d. Enter `Name`, select "LS_BS_DF1" in the `Linked service` and click `Browse` to select the raw file, `API_EN.ATM.CO2E.KT_DS2_en_csv_v2_3358949.csv`. Tick `First row as header`, select `From sample file` and then click `Browse` to select the header file `headers.csv`. Click `OK`.

    ![Dataset configurations](./images/tutorial_1/create_dataset_co2.png)

    e. (Optionally) If you have integrated with Azure DevOps, you can see the `co2` dataset appears in the repository when you save it.

    ![Azure DevOps Repository](./images/tutorial_1/create_dataset_co2_git.png)

    f. Note that `Preview data` do NOT show data in the correct form because the actual data starts at the 6th row.

    ![Preview Dataset](./images/tutorial_1/create_dataset_co2_preview.png)

    g. Verify that the headers appear correctly by clicking `Schema`.

    ![Headers](./images/tutorial_1/create_dataset_co2_headers.png)

    
3. Transform Data
    
    a. Create a data flow to transform the raw file. Click `+ (plus)` button and then select `Data flow`.

    ![Headers](./images/tutorial_1/data_flow.png)

    b. Click `Add Source` to configure the raw file.

    ![Create Data Flow](./images/tutorial_1/create_dataflow_1.png)

    c. Select the `co2` dataset and enter `4` in the `Skip line count`.

    ![Source settings](./images/tutorial_1/create_dataflow_2.png)

    d. Click `Data flow debug` on the top menu and select `Data preview` to check if the raw data appear correctly.

    ![Data preview](./images/tutorial_1/create_dataflow_3.png)

    e. Click `+` sign next to `source1` and select `Unpivot`.

    ![Unpivot](./images/tutorial_1/create_dataflow_4.png)

    f. Add `Country Name, Country Code, Indicator Name and Indicator Code`.

    ![Unpivot settings > Ungroup by](./images/tutorial_1/create_dataflow_5.png)

    g. Enter `year` and set `integer` in the `Unpivot key`.

    ![Unpivot settings > Unpivot key](./images/tutorial_1/create_dataflow_6.png)

    h. Set the unpivoted columns as `co2_emissions` with `string` data type. Tick `Drop rows with null`

    ![Unpivot settings > Unpivot columns](./images/tutorial_1/create_dataflow_7.png)

    i. Click `Data preview` to check if the raw data appear correctly.

    ![Data preview](./images/tutorial_1/create_dataflow_8.png)

    j. Click `+` sign next to `Unpivot1` and select `Sink`.

    ![Sink](./images/tutorial_1/create_dataflow_9.png)

    k. `Sink` defines the output data, un-pivoted CO2 emission. First, create a new linked service, `LS_BS_DF2` like `LS_BS_DF1` to store conformed files in an Azure storage account (i.e. Conformed Layer in the pipeline architecture).
    
    ![Create linked service](./images/tutorial_1/create_linked_service_blob_storage_2.png)

    l. Create a new dataset, `co2_unpivoted` like `co2` dataset.

    ![Create dataset](./images/tutorial_1/data_flow_unpivot.png)

    ![Azure Blob Storage](./images/tutorial_1/create_dataset_blob_storage.png)

    ![DelimitedText](./images/tutorial_1/create_dataset_delimited_text.png)

    ![Dataset configurations](./images/tutorial_1/create_dataset_co2_unpivoted.png)

    m. Select the `co2_unpivoted` dataset.

    ![Sink dataset](./images/tutorial_1/create_dataflow_10.png)

    n. Select `Output to single file` and enter `co2_unpivoted.csv` file name.

    ![Sink settings](./images/tutorial_1/create_dataflow_11.png)

    o. Click `Data preview` to check if the data appear correctly.

    ![Data preview](./images/tutorial_1/create_dataflow_12.png)

    p. Enter the data flow name, `unpivot_dataflow` and click `Save`.

    ![Save](./images/tutorial_1/create_dataflow_13.png)

3. Create a pipeline to orchestrate the data flow.

    a. Click `+ (plus)` button and select `Pipeline`.

    ![Create pipeline](./images/tutorial_1/pipeline.png)
    
    b. Drag and drop `Data flow` onto the right canvas.

    ![Pipeline](./images/tutorial_1/create_pipeline_1_add_data_flow.png)

    c. Name `Unpivot` and select `unpivot_dataflow` in the `Data flow`.

    ![Rename](./images/tutorial_1/create_pipeline_2_rename.png)

    ![Settings](./images/tutorial_1/create_pipeline_3_set_dataflow.png)

    d. Click `Debug` button to run the pipeline and verify the pipeline runs successfully.

    ![Debug](./images/tutorial_1/create_pipeline_4_debug.png)

    ![Result](./images/tutorial_1/create_pipeline_5_result_1.png)

    e. Also, check if the `co2_unpivoted.csv` file is created successfully in the conformed blob storage.

    ![Result](./images/tutorial_1/create_pipeline_6_result_2.png)

## Load Data from Blob to Database

Now the raw file (global CO2 emission) is transformed and stored in the format to be ingested in a database. The picture below illustrates a typical architecture of data pipelines and the red rectangle is covered in this section, where the transformed file is loaded to a database.

![Architecture](./images/tutorial_1/architecture_data_pipeline_conformed_to_db.png)

1. First, create a linked service to connect to the Azure SQL database.

    a. Click `Manage` and `New` to create a new linked service.

    ![Create linked service](./images/tutorial_1/create_new_linked_service.png)

    b. Click `Azure SQL Database` and `Continue`.

    ![Azure SQL Database](./images/tutorial_1/linked_service_sql_server_1.png)

    c. Enter `LS_SQLDW` and the authentication details.

    ![New linked service](./images/tutorial_1/linked_service_sql_server_2.png)

    d. Test the connection. If it fails due to firewall, add the client IP address to the Azure SQL database and `Save`.

    ![Add client IP](./images/tutorial_1/linked_service_sql_server_3.png)

2. Go back to the Data Factory and create a new dataset, `co2_sqldw`, which creates a table, `dev.co2` in the database.

    a. Click `Author`, select `+ (plus)` button and then `Dataset`. Select `Azure SQL Database` and click `Continue`.

    ![Create dataset](./images/tutorial_1/create_dataset_sqlserver_1.png)

    b. Enter `co2_sqldw` and select the linked service, `LS_SQLDW` created above. Set `dev.co2` in the table name. Click `OK` and `Save`.

    ![Config dataset](./images/tutorial_1/create_dataset_sqlserver_2.png)

3. Drag and drop a `Copy data` activity onto the pipeline. Name it `Load to DW` and connect from `Unpivot` to `Load to DW`. 

    ![Copy data](./images/tutorial_1/create_pipeline_7_add_copy_data.png)

4. Select `co2_unpivoted` in the `Source dataset`. Click `Preview data` to ensure that the data appears correctly. Note that we are selecting `Wildcard file path` and enter `* (asterisk)` even though we have only one file, `co2_unpivoted.csv`.

    ![Source](./images/tutorial_1/create_pipeline_8_config_source.png)

5. Configure `Sink`. Select `co2_sqldw` dataset and `Auto create table`.

    ![Source](./images/tutorial_1/create_pipeline_9_config_sink.png)

6. Click `Save` and press `Debug` button to test the pipeline.

    ![Source](./images/tutorial_1/create_pipeline_10_debug.png)

7. Monitor the pipeline and check the result in the database by running a query, `select top 1000 * from dev.co2`.

    ![Source](./images/tutorial_1/create_pipeline_11_result_1.png)

    ![Source](./images/tutorial_1/create_pipeline_12_result_2.png)
