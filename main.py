import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class TransformData(beam.DoFn):
    def process(self, element):
        element["cust_tier_code"] = str(element["cust_tier_code"])
        element["sku"] = int(element["sku"])
        yield element


def run():

    product_views_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }
    sales_amount_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
        ]
    }

    pipeline_options = PipelineOptions(
        temp_location='gs://york_temp_files/tmp/',
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york_temp_files/staging/",
        job_name="nick-racette-final-job-test",
        save_main_session=True
    )



    product_views_out = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_nick_racette",
        tableId="cust_tier_code-sku-total_no_of_product_views"
    )

    sales_amount_out = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_nick_racette",
        tableId="cust_tier_code-sku-total_sales_amount"
    )

    with beam.Pipeline(runner="DataflowRunner", options=pipeline_options) as p:

        sales_amount_tb = p | "Create sales tb" >> beam.io.ReadFromBigQuery(
            query="SELECT c.CUST_TIER_CODE as cust_tier_code, o.SKU as sku, COUNT(o.ORDER_AMT) as total_sales_amount "
                  "FROM `york-cdf-start.final_input_data.customers` as c "
                  "JOIN `york-cdf-start.final_input_data.orders` as o ON c.CUSTOMER_ID = o.CUSTOMER_ID "
                  "GROUP BY sku, cust_tier_code;",
            use_standard_sql=True
        )

        product_views_tb = p | "Create views tb" >> beam.io.ReadFromBigQuery(
            query="SELECT c.CUST_TIER_CODE as cust_tier_code, p.SKU as sku, COUNT(p.SKU) as total_no_of_product_views "
                  "FROM `york-cdf-start.final_input_data.customers` as c "
                  "JOIN `york-cdf-start.final_input_data.product_views` as p ON c.CUSTOMER_ID = p.CUSTOMER_ID "
                  "GROUP BY sku, cust_tier_code;",
            use_standard_sql=True
        )

        transform_sales = sales_amount_tb | "Change sales" >> beam.ParDo(TransformData())
        transform_views = product_views_tb | "Change views" >> beam.ParDo(TransformData())

        transform_sales | "Write sales amount" >> beam.io.WriteToBigQuery(
            sales_amount_out,
            schema=sales_amount_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_nrr/tmp"
        )

        transform_views | "Write product views" >> beam.io.WriteToBigQuery(
            product_views_out,
            schema=product_views_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_nrr/tmp"
        )



if __name__ == "__main__":
    run()
    print("done")
