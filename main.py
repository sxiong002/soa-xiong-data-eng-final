
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions


schema1 = {
    'fields': [
        {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
    ]
}

schema2 = {
    'fields': [
        {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
    ]
}

table1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_soa_xiong',
        tableId='cust_tier_code-sku-total_no_of_product_views',
    )

table2 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_soa_xiong',
        tableId='cust_tier_code-sku-total_sales_amount',
    )


class transform_Data(beam.DoFn):
    def process(self, element):
        # sets cust_tier_code variable type to string variable
        element['cust_tier_code'] = str(element['cust_tier_code'])
        # sets sku variable type to integer variable
        element['sku'] = int(element['sku'])
        yield element


def function():
    # defining the project and bucket location
    beam_options = PipelineOptions(
        project='york-cdf-start',
        region='us-central1',
        runner='DataflowRunner',
        temp_location='gs://york-project-bucket/soa-xiong/tmp',
        staging_location='gs://york-project-bucket/soa-xiong/staging',
        job_name='soa-xiong-final-job',
    )

    with beam.Pipeline(options=beam_options) as pipeline:
        data1 = pipeline | 'Read data1 from BigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT c.CUST_TIER_CODE AS cust_tier_code, p.sku AS sku, COUNT(*) AS total_no_of_product_views '
                  'FROM `york-cdf-start.final_input_data.customers` as c '
                  'INNER JOIN `york-cdf-start.final_input_data.product_views` as p '
                  'ON c.customer_id = p.customer_id '
                  'GROUP BY cust_tier_code, sku '
                  'ORDER BY total_no_of_product_views DESC ',
            use_standard_sql=True
        )

        data2 = pipeline | 'Read data2 from BigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT c.CUST_TIER_CODE AS cust_tier_code, o.sku AS sku, ROUND(SUM(o.ORDER_AMT), 2) AS total_sales_amount '
                  'FROM `york-cdf-start.final_input_data.customers` as c '
                  'INNER JOIN `york-cdf-start.final_input_data.orders` as o '
                  'ON c.customer_id = o.customer_id '
                  'GROUP BY cust_tier_code, sku '
                  'ORDER BY total_sales_amount DESC',
            use_standard_sql=True
        )

        transform1 = data1 | 'Transform Data1' >> beam.ParDo(transform_Data())
        transform2 = data2 | 'Transform Data2' >> beam.ParDo(transform_Data())

        transform1 | 'write table1 to BigQuery' >> beam.io.WriteToBigQuery(
            table1,
            schema=schema1,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        transform2 | 'write table2 to BigQuery' >> beam.io.WriteToBigQuery(
            table2,
            schema=schema2,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == '__main__':
    function()
