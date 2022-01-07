import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


# class TransformData(beam.DoFn):
#     def process(self, element):
#         yield element


def run():
    customer_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CUST_TIER_CODE', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }
    product_views_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'EVENT_TM', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'SKU', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
    orders_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'TRANS_TM', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'ORDER_NBR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'SKU', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'ORDER_AMT', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    }
    # product_views_schema = {
    #     'fields': [
    #         {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    #         {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
    #     ]
    # }
    # sales_amount_schema = {
    #     'fields': [
    #         {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    #         {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
    #     ]
    # }

    pipeline_options = PipelineOptions(
        temp_location='gs://york_nrr/tmp/',
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york_nrr/staging/",
        job_name="nick-racette-final-job-test",
        save_main_session=True
    )

    customers_out = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_nick_racette_test",
        tableId="customers_out"
    )
    product_views_out = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_nick_racette_test",
        tableId="product_views_out"
    )
    orders_out = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_nick_racette_test",
        tableId="orders_out"
    )

    # product_views_out = bigquery.TableReference(
    #     projectId="york-cdf-start",
    #     datasetId="final_nick_racette",
    #     tableId="cust_tier_code-sku-total_no_of_product_views"
    # )
    #
    # sales_amount_out = bigquery.TableReference(
    #     projectId="york-cdf-start",
    #     datasetId="final_nick_racette",
    #     tableId="cust_tier_code-sku-total_sales_amount"
    # )

    with beam.Pipeline(runner="DataflowRunner", options=pipeline_options) as p:
        customers_tb = p | "Read in customers" >> beam.io.ReadFromBigQuery(
            gcs_location="gs://york_nrr/tmp/",
            table="york-cdf-start:final_input_data.customers"
        )
        product_views_tb = p | "Read in product views" >> beam.io.ReadFromBigQuery(
            gcs_location="gs://york_nrr/tmp/",
            table="york-cdf-start:final_input_data.product_views"
        )
        order_tb = p | "Read in orders" >> beam.io.ReadFromBigQuery(
            gcs_location="gs://york_nrr/tmp/",
            table="york-cdf-start:final_input_data.orders"
        )
        # product_views_tb = p | "Create product_views_tb" >> beam.io.ReadFromBigQuery(
        #     gcs_location="gs://york_temp_files/tmp/",
        #     table="york-cdf-start:final_input_data.customers"
        # )
        #
        # sales_amount_tb = p | "Create sales_amount_tb" >> beam.io.ReadFromBigQuery(
        #     gcs_location="gs://york_temp_files/tmp/",
        #     table="york-cdf-start:final_input_data.customers"
        # )

        # product_views_tb | "Write product views" >> beam.io.WriteToBigQuery(
        #     product_views_out,
        #     schema=product_views_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     custom_gcs_temp_location="gs://york_temp_files/tmp"
        # )
        #
        # sales_amount_tb | "Write sales amount" >> beam.io.WriteToBigQuery(
        #     sales_amount_out,
        #     schema=sales_amount_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     custom_gcs_temp_location="gs://york_temp_files/tmp"
        # )
        customers_tb | "Write customers table" >> beam.io.WriteToBigQuery(
            customers_out,
            schema=customer_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_nrr/tmp"
        )
        product_views_tb | "Write product views table" >> beam.io.WriteToBigQuery(
            product_views_out,
            schema=product_views_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_nrr/tmp"
        )
        order_tb | "Write orders table" >> beam.io.WriteToBigQuery(
            orders_out,
            schema=orders_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_nrr/tmp"
        )


if __name__ == "__main__":
    run()
    print("done")
