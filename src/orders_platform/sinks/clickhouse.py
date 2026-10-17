from datetime import timezone


def insert_customers_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    customer_columns = [
        'customer_id',
        'email',
        'region',
        'segment',
        'created_at',
        'updated_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*customer_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in customer_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.customers_cdc',
                data=buffer,
                column_names=customer_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
                table='staging.customers_cdc',
            data=buffer,
            column_names=customer_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows