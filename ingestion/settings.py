TABLES_CONFIG = {
    'sales_transactions': {
        "file_format": '.json',
        "export_path": 'export/sales_transactions.parquet',
        "export_query": 
        """copy (
            select      md5(order_id || unnest.item_id) as sk,
                        order_id::integer as order_id,
                        timestamp::timestamp as timestamp,
                        completed_timestamp::timestamp as completed_timestamp,
                        status,
                        customer_info.user_id::integer as user_id,
                        customer_info.address_id::integer as address_id,
                        coalesce(customer_info.customer_email) as customer_email,
                        unnest.item_id::integer as item_id,
                        unnest.quantity::integer as quantity,
                        unnest.unit_price::numeric(10,2) as unit_price,
                        (unnest.unit_price::numeric(10,2) * unnest.quantity::integer)::numeric(10,2) as item_value
            from        sales_transactions
            left join   unnest(items) AS item on 1=1 --keep null items if possible
        ) to ? (format PARQUET, compression 'SNAPPY');
        """
    }
}